package main

import (
	"context"
	"log"
	"os"
	"os/signal"

	"github.com/kamilwrzyszcz/kafka_example/listener/reader"
	"github.com/kamilwrzyszcz/kafka_example/util"
	kafkago "github.com/segmentio/kafka-go"
	"golang.org/x/sync/errgroup"
)

func main() {
	config, err := util.LoadConfig(".")
	if err != nil {
		log.Fatal("cannot load config: ", err)
	}

	reader := reader.NewKafkaReader(config)

	messageCommitChan := make(chan kafkago.Message)

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt)
	defer stop()
	g, _ := errgroup.WithContext(ctx)

	g.Go(func() error {
		return reader.FetchMessage(ctx, messageCommitChan)
	})
	g.Go(func() error {
		return reader.CommitMessages(ctx, messageCommitChan)
	})

	err = g.Wait()
	if err != nil {
		log.Fatalln(err)
	}
}
