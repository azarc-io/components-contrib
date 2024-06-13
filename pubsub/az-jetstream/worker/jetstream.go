/*
Copyright 2021 The Dapr Authors
Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at
    http://www.apache.org/licenses/LICENSE-2.0
Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

package worker

import (
	"context"
	"errors"
	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"
	"github.com/nats-io/nkeys"
	"reflect"
	"sync"
	"sync/atomic"

	mdutils "github.com/dapr/components-contrib/metadata"
	"github.com/dapr/components-contrib/pubsub"
	"github.com/dapr/kit/logger"
	"github.com/dapr/kit/retry"
)

type jetstreamPubSub struct {
	nc   *nats.Conn
	l    logger.Logger
	meta metadata

	backOffConfig retry.Config

	closed     atomic.Bool
	closeCh    chan struct{}
	wg         sync.WaitGroup
	jsc        jetstream.JetStream
	stream     jetstream.Stream
	consumeCtx jetstream.ConsumeContext
}

func NewJetStream(logger logger.Logger) pubsub.PubSub {
	return &jetstreamPubSub{
		l:       logger,
		closeCh: make(chan struct{}),
	}
}

func (js *jetstreamPubSub) Init(_ context.Context, metadata pubsub.Metadata) error {
	var err error
	js.meta, err = parseMetadata(metadata)
	if err != nil {
		return err
	}

	var opts []nats.Option
	opts = append(opts, nats.Name(js.meta.Name))

	// Set nats.UserJWT options when jwt and seed key is provided.
	if js.meta.Jwt != "" && js.meta.SeedKey != "" {
		opts = append(opts, nats.UserJWT(func() (string, error) {
			return js.meta.Jwt, nil
		}, func(nonce []byte) ([]byte, error) {
			return sigHandler(js.meta.SeedKey, nonce)
		}))
	} else if js.meta.TLSClientCert != "" && js.meta.TLSClientKey != "" {
		js.l.Debug("Configure nats for tls client authentication")
		opts = append(opts, nats.ClientCert(js.meta.TLSClientCert, js.meta.TLSClientKey))
	} else if js.meta.Token != "" {
		js.l.Debug("Configure nats for token authentication")
		opts = append(opts, nats.Token(js.meta.Token))
	}

	js.nc, err = nats.Connect(js.meta.NatsURL, opts...)
	if err != nil {
		return err
	}
	js.l.Debugf("Connected to nats at %s", js.meta.NatsURL)

	jsOpts := []nats.JSOpt{}

	if js.meta.Domain != "" {
		jsOpts = append(jsOpts, nats.Domain(js.meta.Domain))
	}

	if js.meta.APIPrefix != "" {
		jsOpts = append(jsOpts, nats.APIPrefix(js.meta.APIPrefix))
	}

	if js.jsc, err = jetstream.New(js.nc); err != nil {
		return err
	}

	js.l.Infof("creating stream with subjects: %v", js.meta.Subjects)

	if js.stream, err = js.jsc.CreateOrUpdateStream(context.Background(), jetstream.StreamConfig{
		Name:        js.meta.StreamName,
		Description: js.meta.Name,
		Retention:   jetstream.WorkQueuePolicy,
		Subjects:    js.meta.Subjects,
		MaxAge:      js.meta.MaxAge,
	}); err != nil {
		js.l.Errorf("failed to create stream: %s", err)
		return err
	}

	// Default retry configuration is used if no backOff properties are set.
	if err := retry.DecodeConfigWithPrefix(
		&js.backOffConfig,
		metadata.Properties,
		"backOff"); err != nil {
		return err
	}

	js.l.Debug("JetStream initialization complete")

	return nil
}

func (js *jetstreamPubSub) Features() []pubsub.Feature {
	return nil
}

func (js *jetstreamPubSub) Publish(ctx context.Context, req *pubsub.PublishRequest) error {
	if js.closed.Load() {
		return errors.New("component is closed")
	}

	var opts []jetstream.PublishOpt
	var msgID string

	event, err := pubsub.FromCloudEvent(req.Data, "", "", "", "")
	if err != nil {
		js.l.Debugf("error unmarshalling cloudevent: %v", err)
	} else {
		// Use the cloudevent id as the Nats-MsgId for deduplication
		if id, ok := event["id"].(string); ok {
			msgID = id
			opts = append(opts, jetstream.WithMsgID(msgID))
		}
	}

	if msgID == "" {
		js.l.Warn("empty message ID, Jetstream deduplication will not be possible")
	}

	js.l.Debugf("Publishing to topic %v id: %s", req.Topic, msgID)
	_, err = js.jsc.Publish(ctx, req.Topic, req.Data, opts...)

	if err != nil {
		js.l.Errorf("failed to publish message: %s", err)
	}

	return err
}

func (js *jetstreamPubSub) Subscribe(ctx context.Context, req pubsub.SubscribeRequest, handler pubsub.Handler) error {
	if js.closed.Load() {
		return errors.New("component is closed")
	}

	var consumerConfig jetstream.ConsumerConfig

	if v := js.meta.DurableName; v != "" {
		consumerConfig.Durable = v
	}
	if v, ok := req.Metadata["durableName"]; ok {
		consumerConfig.Durable = v
	}

	if v := js.meta.ConsumerName; v != "" {
		consumerConfig.Name = v
	}
	if v, ok := req.Metadata["consumerName"]; ok {
		consumerConfig.Name = v
	}

	if v := js.meta.internalStartTime; !v.IsZero() {
		consumerConfig.OptStartTime = &v
	}

	if v := js.meta.StartSequence; v > 0 {
		consumerConfig.OptStartSeq = v
	}

	if js.meta.AckWait != 0 {
		consumerConfig.AckWait = js.meta.AckWait
	}

	if js.meta.MaxDeliver != 0 {
		consumerConfig.MaxDeliver = js.meta.MaxDeliver
	}

	if len(js.meta.BackOff) != 0 {
		consumerConfig.BackOff = js.meta.BackOff
	}

	if js.meta.MaxAckPending != 0 {
		consumerConfig.MaxAckPending = js.meta.MaxAckPending
	}

	if js.meta.Replicas != 0 {
		consumerConfig.Replicas = js.meta.Replicas
	}

	if js.meta.MemoryStorage {
		consumerConfig.MemoryStorage = true
	}

	if js.meta.RateLimit != 0 {
		consumerConfig.RateLimit = js.meta.RateLimit
	}

	if js.meta.InactiveThreshold != 0 {
		consumerConfig.InactiveThreshold = js.meta.InactiveThreshold
	}

	consumerConfig.AckPolicy = js.meta.internalAckPolicy
	consumerConfig.FilterSubject = req.Topic
	consumerConfig.DeliverPolicy = js.meta.internalDeliverPolicy

	js.l.Infof("metadata %v", req.Metadata)

	natsHandler := func(m jetstream.Msg) {
		jsm, err := m.Metadata()
		if err != nil {
			// If we get an error, then we don't have a valid JetStream message.
			js.l.Error(err)
			return
		}

		js.l.Debugf("Processing JetStream message %s/%d", m.Subject, jsm.Sequence)
		err = handler(ctx, &pubsub.NewMessage{
			Topic: req.Topic,
			Data:  m.Data(),
			Metadata: map[string]string{
				"Topic": m.Subject(),
			},
		})
		if err != nil {
			js.l.Errorf("Error processing JetStream message %s/%d: %v", m.Subject, jsm.Sequence, err)

			if js.meta.internalAckPolicy == jetstream.AckExplicitPolicy || js.meta.internalAckPolicy == jetstream.AckAllPolicy {
				var nakErr error
				if js.meta.AckWait != 0 {
					nakErr = m.NakWithDelay(js.meta.AckWait)
				} else {
					nakErr = m.Nak()
				}
				if nakErr != nil {
					js.l.Errorf("Error while sending NAK for JetStream message %s/%d: %v", m.Subject, jsm.Sequence, nakErr)
				}
			}

			return
		}

		if js.meta.internalAckPolicy == jetstream.AckExplicitPolicy || js.meta.internalAckPolicy == jetstream.AckAllPolicy {
			err = m.Ack()
			if err != nil {
				js.l.Errorf("Error while sending ACK for JetStream message %s/%d: %v", m.Subject, jsm.Sequence, err)
			}
		}
	}

	// Choose the correct handler based on the concurrency model.
	var concHandler jetstream.MessageHandler
	switch js.meta.Concurrency {
	case pubsub.Single:
		concHandler = natsHandler
	case pubsub.Parallel:
		concHandler = func(msg jetstream.Msg) {
			js.wg.Add(1)
			go func() {
				natsHandler(msg)
				js.wg.Done()
			}()
		}
	}

	var err error
	streamName := js.meta.StreamName
	if streamName == "" {
		streamName, err = js.jsc.StreamNameBySubject(ctx, req.Topic)
		if err != nil {
			js.l.Errorf("could not fetch stream: %s", err)
			return err
		}
	}

	consumerInfo, err := js.jsc.CreateOrUpdateConsumer(ctx, streamName, consumerConfig)
	if err != nil {
		js.l.Errorf("could not add consumer: %s", err)
		return err
	}

	js.consumeCtx, err = consumerInfo.Consume(concHandler)
	if err != nil {
		js.l.Errorf("nats: could not subscribe: %s", err)
		return err
	}

	js.wg.Add(1)
	go func() {
		defer js.wg.Done()
		select {
		case <-ctx.Done():
		case <-js.closeCh:
		}
		js.consumeCtx.Drain()
	}()

	return nil
}

func (js *jetstreamPubSub) Close() error {
	defer js.wg.Wait()
	if js.closed.CompareAndSwap(false, true) {
		close(js.closeCh)
	}
	return js.nc.Drain()
}

// Handle nats signature request for challenge response authentication.
func sigHandler(seedKey string, nonce []byte) ([]byte, error) {
	kp, err := nkeys.FromSeed([]byte(seedKey))
	if err != nil {
		return nil, err
	}
	// Wipe our key on exit.
	defer kp.Wipe()

	sig, _ := kp.Sign(nonce)
	return sig, nil
}

// GetComponentMetadata returns the metadata of the component.
func (js *jetstreamPubSub) GetComponentMetadata() (metadataInfo mdutils.MetadataMap) {
	metadataStruct := metadata{}
	mdutils.GetMetadataInfoFromStructType(reflect.TypeOf(metadataStruct), &metadataInfo, mdutils.PubSubType)
	return
}
