package main

import (
	"context"
	"log"
	"os"
	"os/signal"
	"syscall"

	"github.com/lev-stas/beaver/internal/config"
	"github.com/lev-stas/beaver/internal/consumer"
)

func main() {
	cfg := config.MustLoadConsumer()

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	if err := consumer.Run(ctx, cfg); err != nil {
		log.Fatal(err)
	}
}
