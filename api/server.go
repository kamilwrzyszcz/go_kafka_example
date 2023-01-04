package api

import (
	"context"

	"github.com/gofiber/fiber"
	"github.com/kamilwrzyszcz/kafka_example/api/writer"
	"github.com/kamilwrzyszcz/kafka_example/util"
	kafkago "github.com/segmentio/kafka-go"
	"golang.org/x/sync/errgroup"
)

type Server struct {
	router *fiber.App
	writer *writer.KafkaWriter
	config util.Config
}

func NewServer(config util.Config) *Server {
	writer := writer.NewKafkaWriter(config)

	server := &Server{
		writer: writer,
		config: config,
	}
	server.setUpRouter()

	return server
}

func (server *Server) setUpRouter() {
	router := fiber.New()

	router.Post("/messages", server.messages)

	server.router = router
}

type messagesRequest struct {
	Messages []message `json:"messages"`
}

type message struct {
	Value string `json:"value"`
}

func (server *Server) messages(c *fiber.Ctx) {
	var req messagesRequest
	if err := c.BodyParser(&req); err != nil {
		c.Status(fiber.StatusBadRequest).JSON(&fiber.Map{
			"errors": err.Error(),
		})
		return
	}

	ctx := context.Background()
	// Could also be done with WaitGroup
	g, ctx := errgroup.WithContext(ctx)

	for _, msg := range req.Messages {
		m := kafkago.Message{
			Value: []byte(msg.Value),
		}

		g.Go(func() error {
			return server.writer.WriteMessage(ctx, m)
		})
	}

	err := g.Wait()
	if err != nil {
		c.Status(fiber.StatusInternalServerError).JSON(&fiber.Map{
			"errors": err.Error(),
		})
		return
	}

	c.Status(fiber.StatusOK)
}

func (server *Server) Start() error {
	return server.router.Listen(server.config.ServerAddress)
}

func (server *Server) Shutdown() error {
	ctx := context.Background()
	g, _ := errgroup.WithContext(ctx)

	g.Go(func() error {
		return server.writer.Shutdown()
	})
	g.Go(func() error {
		return server.router.Shutdown()
	})

	return g.Wait()
}
