package main

// TODO:
/*
Dwie apki
Jedna: proste api, jeden endpoint
zgarnia jsona z lista wiadomosci, wiadomosci sa jedna po drugiej wysylane przez writer (waitgroup w go rutynie? errory w channel i sprawdzic na koncu? albo tez errgroup jak w readerze: https://stackoverflow.com/questions/62387307/how-to-catch-errors-from-goroutines)
Druga: oddzielnie odpalany reader i commit wypisujacy wiadomosci do terminala

Tym razem zrob config
*/

import (
	"fmt"
	"log"
	"os"
	"os/signal"

	"github.com/kamilwrzyszcz/kafka_example/api"
	"github.com/kamilwrzyszcz/kafka_example/util"
)

func main() {
	config, err := util.LoadConfig(".")
	if err != nil {
		log.Fatal("cannot load config: ", err)
	}

	server := api.NewServer(config)

	c := make(chan os.Signal, 1)
	signal.Notify(c, os.Interrupt)
	go func() {
		_ = <-c
		fmt.Println("Gracefully shutting down...")
		err = server.Shutdown()
		if err != nil {
			log.Panic(err)
		}
	}()

	if err := server.Start(); err != nil {
		log.Panic(err)
	}
}
