package main

import (
	"context"
	"log"
	"os"
	"os/signal"
	"syscall"

	"github.com/lev-stas/beaver/internal/config"
	"github.com/lev-stas/beaver/internal/producer"
)

func main() {
	cfg := config.MustLoadProducer()

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	if err := producer.Run(ctx, cfg); err != nil {
		log.Fatal(err)
	}
}
