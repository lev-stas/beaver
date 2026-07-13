package producer

import (
	"context"
	"errors"
	"fmt"
	"log"
	"sync"
	"time"

	"github.com/lev-stas/beaver/internal/config"
	"github.com/r3labs/sse/v2"
	"github.com/twmb/franz-go/pkg/kadm"
	"github.com/twmb/franz-go/pkg/kerr"
	"github.com/twmb/franz-go/pkg/kgo"
	"gopkg.in/cenkalti/backoff.v1"
)

func Run(ctx context.Context, cfg *config.ProducerConfig) error {
	sseClient := sse.NewClient(cfg.Producer.SSEURL)
	sseClient.Headers["User-Agent"] = cfg.Producer.UserAgent
	sseClient.ReconnectStrategy = backoff.WithContext(backoff.NewExponentialBackOff(), ctx)

	tctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	produceCtx := context.Background()

	opts := []kgo.Opt{
		kgo.SeedBrokers(cfg.Kafka.Brokers...),
		kgo.ProducerBatchCompression(compressionCodec(cfg.Kafka.Compression)),
	}

	kclient, err := kgo.NewClient(opts...)
	if err != nil {
		return fmt.Errorf("creating kafka client: %w", err)
	}
	defer kclient.Close()

	if err := ensureTopic(tctx, kclient, cfg.Kafka.Topic, cfg.Kafka.Partitions, cfg.Kafka.ReplicationFactor); err != nil {
		return err
	}

	const eventsBufferSize = 1000
	events := make(chan []byte, eventsBufferSize)

	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()
		batchProducer(produceCtx, kclient, cfg.Kafka.Topic, events, cfg.Producer.BatchSize, cfg.Producer.FlushInterval)
	}()

	err = sseClient.SubscribeWithContext(ctx, "messages", func(msg *sse.Event) {
		events <- msg.Data
		fmt.Println(string(msg.ID))
	})
	if err != nil && ctx.Err() == nil {
		return fmt.Errorf("subscribing to sse stream: %w", err)
	}

	log.Println("Shutdown signal received, flushing remaining events...")
	close(events)
	wg.Wait()
	log.Println("Producer stopped")

	return nil
}

func ensureTopic(ctx context.Context, kclient *kgo.Client, topic string, partitions int32, replicationFactor int16) error {
	adm := kadm.NewClient(kclient)

	topicResp, err := adm.CreateTopics(ctx, partitions, replicationFactor, nil, topic)
	if err != nil {
		return fmt.Errorf("creating topic: %w", err)
	}

	if _, err := topicResp.On(topic, nil); err != nil {
		if errors.Is(err, kerr.TopicAlreadyExists) {
			log.Printf("Topic %s already exists", topic)
			return nil
		}
		return fmt.Errorf("topic %s does not exist: %w", topic, err)
	}

	log.Printf("Kafka topic created. Topic name: %s", topic)
	return nil
}

func batchProducer(ctx context.Context, kclient *kgo.Client, topic string, events <-chan []byte, batchSize int, flushInterval time.Duration) {
	ticker := time.NewTicker(flushInterval)
	defer ticker.Stop()

	batch := make([]*kgo.Record, 0, batchSize)

	flush := func() {
		if len(batch) == 0 {
			return
		}

		results := kclient.ProduceSync(ctx, batch...)
		if err := results.FirstErr(); err != nil {
			log.Printf("Error sending batch to kafka: %v", err)

			if errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) {
				log.Println("Request canceled by context")
			}
		}

		batch = batch[:0]
	}

	for {
		select {
		case data, ok := <-events:
			if !ok {
				flush()
				return
			}

			batch = append(batch, &kgo.Record{Topic: topic, Value: data})
			if len(batch) >= batchSize {
				flush()
			}

		case <-ticker.C:
			flush()
		}
	}
}

func compressionCodec(name string) kgo.CompressionCodec {
	switch name {
	case "gzip":
		return kgo.GzipCompression()
	case "snappy":
		return kgo.SnappyCompression()
	case "lz4":
		return kgo.Lz4Compression()
	case "zstd":
		return kgo.ZstdCompression()
	default:
		return kgo.NoCompression()
	}
}
