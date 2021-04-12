package rabbitmqout

import (
	"context"
	"fmt"
	"os"
	"path/filepath"

	"github.com/elastic/beats/v7/libbeat/beat"
	"github.com/elastic/beats/v7/libbeat/common"
	"github.com/elastic/beats/v7/libbeat/common/file"
	"github.com/elastic/beats/v7/libbeat/logp"
	"github.com/elastic/beats/v7/libbeat/outputs"
	"github.com/elastic/beats/v7/libbeat/outputs/codec"
	"github.com/elastic/beats/v7/libbeat/publisher"
	"github.com/koding/logging"
	"github.com/koding/rabbitmq"
	"github.com/streadway/amqp"
)

func init() {
	outputs.RegisterType("rabbitmq", rabbitmqout)
}

type rabbitmqOutput struct {
	log      *logp.Logger
	filePath string
	beat     beat.Info
	observer outputs.Observer
	rotator  *file.Rotator
	codec    codec.Codec
}

// makeFileout instantiates a new file output instance.
func rabbitmqout(
	_ outputs.IndexManager,
	beat beat.Info,
	observer outputs.Observer,
	cfg *common.Config,
) (outputs.Group, error) {
	config := defaultConfig
	if err := cfg.Unpack(&config); err != nil {
		return outputs.Fail(err)
	}

	// disable bulk support in publisher pipeline
	cfg.SetInt("bulk_max_size", -1, -1)

	fo := &rabbitmqOutput{
		log:      logp.NewLogger("file"),
		beat:     beat,
		observer: observer,
	}
	if err := fo.init(beat, config); err != nil {
		return outputs.Fail(err)
	}

	return outputs.Success(-1, 0, fo)
}

func (out *rabbitmqOutput) init(beat beat.Info, c config) error {
	var path string
	if c.Filename != "" {
		path = filepath.Join(c.Path, c.Filename)
	} else {
		path = filepath.Join(c.Path, out.beat.Beat)
	}

	out.filePath = path

	var err error
	out.rotator, err = file.NewFileRotator(
		path,
		file.MaxSizeBytes(c.RotateEveryKb*1024),
		file.MaxBackups(c.NumberOfFiles),
		file.Permissions(os.FileMode(c.Permissions)),
		file.RotateOnStartup(c.RotateOnStartup),
		file.WithLogger(logp.NewLogger("rotator").With(logp.Namespace("rotator"))),
	)
	if err != nil {
		return err
	}

	out.codec, err = codec.CreateEncoder(beat, c.Codec)
	if err != nil {
		return err
	}

	out.log.Infof("Initialized file output. "+
		"path=%v max_size_bytes=%v max_backups=%v permissions=%v",
		path, c.RotateEveryKb*1024, c.NumberOfFiles, os.FileMode(c.Permissions))

	return nil
}

// Implement Outputer
func (out *rabbitmqOutput) Close() error {
	return out.rotator.Close()
}

func (out *rabbitmqOutput) Publish(_ context.Context, batch publisher.Batch) error {
	defer batch.ACK()

	st := out.observer
	events := batch.Events()
	st.NewBatch(len(events))

	dropped := 0
	for i := range events {
		event := &events[i]

		serializedEvent, err := out.codec.Encode(out.beat.Beat, &event.Content)
		if err != nil {
			if event.Guaranteed() {
				out.log.Errorf("Failed to serialize the event: %+v", err)
			} else {
				out.log.Warnf("Failed to serialize the event: %+v", err)
			}
			out.log.Debugf("Failed event: %v", event)

			dropped++
			continue
		}

		if err = send(serializedEvent); err != nil {
			st.WriteError(err)

			if event.Guaranteed() {
				out.log.Errorf("Writing event to file failed with: %+v", err)
			} else {
				out.log.Warnf("Writing event to file failed with: %+v", err)
			}

			dropped++
			continue
		}

		st.WriteBytes(len(serializedEvent) + 1)
	}

	st.Dropped(dropped)
	st.Acked(len(events) - dropped)

	return nil
}

func (out *rabbitmqOutput) String() string {
	return "rabbitmq"
}

func send(data []byte) error {
	rlog := logging.NewLogger("rabbit-snps-send")
	rhost := "de02-sevan2"
	port := 5672
	username := "elk"
	password := "r@bbit4elk"
	vhost := "elk"
	exchange := "elk-test"
	routingKey := "elk-test-key"

	rmq := rabbitmq.New(
		&rabbitmq.Config{
			Host:     rhost,
			Port:     port,
			Username: username,
			Password: password,
			Vhost:    vhost,
		},
		rlog,
	)

	rExchange := rabbitmq.Exchange{
		Name: exchange,
	}

	queue := rabbitmq.Queue{}
	publishingOptions := rabbitmq.PublishingOptions{
		//Tag:        "ProducerTag",
		RoutingKey: routingKey,
	}

	publisher, err := rmq.NewProducer(rExchange, queue, publishingOptions)
	if err != nil {
		panic(err)
	}
	defer publisher.Shutdown()
	publisher.RegisterSignalHandler()

	// may be we should autoconvert to byte array?
	msg := amqp.Publishing{
		DeliveryMode: 2,
		Body:         data,
	}

	publisher.NotifyReturn(func(message amqp.Return) {
		fmt.Println(message)
	})

	err = publisher.Publish(msg)
	if err != nil {
		fmt.Println(err)
	}
}
