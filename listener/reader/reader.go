package reader

import (
	"context"
	"log"

	"github.com/kamilwrzyszcz/kafka_example/util"
	kafkago "github.com/segmentio/kafka-go"
)

type KafkaReader struct {
	reader *kafkago.Reader
}

func NewKafkaReader(config util.Config) *KafkaReader {
	return &KafkaReader{
		reader: kafkago.NewReader(kafkago.ReaderConfig{
			Brokers: []string{config.KafkaBroker},
			Topic:   config.KafkaTopic,
			GroupID: config.GroupId,
		}),
	}
}

func (k *KafkaReader) FetchMessage(ctx context.Context, messageCommitChan chan kafkago.Message) error {
	for {
		select {
		case <-ctx.Done():
			log.Printf("Reader, context done: %v\n", ctx.Err())
			return ctx.Err()
		default:
			message, err := k.reader.FetchMessage(ctx)
			if err != nil {
				return err
			}
			log.Printf("Message received: %v\n", string(message.Value))

			select {
			case <-ctx.Done():
			case messageCommitChan <- message:
			}
		}
	}
}

func (k *KafkaReader) CommitMessages(ctx context.Context, messageCommitChan <-chan kafkago.Message) error {
	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case msg := <-messageCommitChan:
			err := k.reader.CommitMessages(ctx, msg)
			if err != nil {
				return err
			}
			log.Printf("Message commited: %v\n", string(msg.Value))
		}
	}
}
