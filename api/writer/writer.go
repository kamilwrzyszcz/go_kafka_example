package writer

import (
	"context"
	"log"

	"github.com/kamilwrzyszcz/kafka_example/util"
	kafkago "github.com/segmentio/kafka-go"
)

type KafkaWriter struct {
	writer *kafkago.Writer
}

func NewKafkaWriter(config util.Config) *KafkaWriter {
	return &KafkaWriter{
		writer: &kafkago.Writer{
			Addr:                   kafkago.TCP(config.KafkaBroker),
			Topic:                  config.KafkaTopic,
			AllowAutoTopicCreation: true,
		},
	}
}

func (k *KafkaWriter) WriteMessage(
	ctx context.Context,
	message kafkago.Message,
) error {
	err := k.writer.WriteMessages(ctx, message)
	if err != nil {
		return err
	}

	log.Printf("Message send: %v\n", string(message.Value))
	return nil
}

func (k *KafkaWriter) Shutdown() error {
	return k.writer.Close()
}
