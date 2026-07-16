package config

import (
	"flag"
	"fmt"
	"time"

	"github.com/ilyakaznacheev/cleanenv"
)

type Kafka struct {
	Brokers           []string `yaml:"brokers"`
	Topic             string   `yaml:"topic"`
	Compression       string   `yaml:"compression"`
	Partitions        int32    `yaml:"partitions"`
	ReplicationFactor int16    `yaml:"replication-factor"`
}

func (k *Kafka) Validate() error {
	if len(k.Brokers) == 0 {
		return fmt.Errorf("kafka.brokers is required")
	}

	if k.Topic == "" {
		return fmt.Errorf("kafka.topic is required")
	}

	return nil
}

type Metrics struct {
	ListenAddress string `yaml:"listen-address"`
}

func (m *Metrics) Validate() error {
	if m.ListenAddress == "" {
		return fmt.Errorf("metrics.listen-address is required")
	}

	return nil
}

type ProducerConfig struct {
	Kafka    Kafka   `yaml:"kafka"`
	Metrics  Metrics `yaml:"metrics"`
	Producer struct {
		SSEURL        string        `yaml:"sse-url"`
		UserAgent     string        `yaml:"user-agent"`
		BatchSize     int           `yaml:"batch-size"`
		FlushInterval time.Duration `yaml:"flush-interval"`
	} `yaml:"producer"`
}

func (c *ProducerConfig) Validate() error {
	if err := c.Kafka.Validate(); err != nil {
		return err
	}

	if err := c.Metrics.Validate(); err != nil {
		return err
	}

	if c.Kafka.Partitions <= 0 {
		return fmt.Errorf("kafka.partitions must be 1 or greater")
	}

	if c.Kafka.ReplicationFactor <= 0 {
		return fmt.Errorf("kafka.replication-factor must be 1 or greater")
	}

	if c.Producer.SSEURL == "" {
		return fmt.Errorf("producer.sse-url is required")
	}

	if c.Producer.UserAgent == "" {
		return fmt.Errorf("producer.user-agent is required")
	}

	if c.Producer.BatchSize <= 0 {
		return fmt.Errorf("producer.batch-size must be 1 or greater")
	}

	if c.Producer.FlushInterval <= 0 {
		return fmt.Errorf("producer.flush-interval must be greater than 0")
	}

	return nil
}

type ClickHouse struct {
	Address  string `yaml:"address"`
	Database string `yaml:"database"`
	Table    string `yaml:"table"`
}

func (ch *ClickHouse) Validate() error {
	if ch.Address == "" {
		return fmt.Errorf("clickhouse.address is required")
	}

	if ch.Database == "" {
		return fmt.Errorf("clickhouse.database is required")
	}

	if ch.Table == "" {
		return fmt.Errorf("clickhouse.table is required")
	}

	return nil
}

type Postgres struct {
	DSN   string `yaml:"dsn"`
	Table string `yaml:"table"`
}

func (p *Postgres) Validate() error {
	if p.DSN == "" {
		return fmt.Errorf("postgres.dsn is required")
	}

	if p.Table == "" {
		return fmt.Errorf("postgres.table is required")
	}

	return nil
}

type ConsumerConfig struct {
	Kafka      Kafka      `yaml:"kafka"`
	ClickHouse ClickHouse `yaml:"clickhouse"`
	Postgres   Postgres   `yaml:"postgres"`
	Metrics    Metrics    `yaml:"metrics"`
	Consumer   struct {
		GroupName string `yaml:"consumer-group"`
	} `yaml:"consumer"`
}

func (c *ConsumerConfig) Validate() error {
	if err := c.Kafka.Validate(); err != nil {
		return err
	}

	if err := c.ClickHouse.Validate(); err != nil {
		return err
	}

	if err := c.Postgres.Validate(); err != nil {
		return err
	}

	if err := c.Metrics.Validate(); err != nil {
		return err
	}

	if c.Consumer.GroupName == "" {
		return fmt.Errorf("consumer.consumer-group is required")
	}

	return nil
}

func MustLoadProducer() *ProducerConfig {
	var cfg ProducerConfig
	path := fetchConfigPath()

	if err := cleanenv.ReadConfig(path, &cfg); err != nil {
		panic(fmt.Errorf("read config file %q: %w", path, err))
	}
	if err := cfg.Validate(); err != nil {
		panic(fmt.Errorf("invalid config: %w", err))
	}
	return &cfg
}

func MustLoadConsumer() *ConsumerConfig {
	var cfg ConsumerConfig
	path := fetchConfigPath()

	if err := cleanenv.ReadConfig(path, &cfg); err != nil {
		panic(fmt.Errorf("read config file %q: %w", path, err))
	}
	if err := cfg.Validate(); err != nil {
		panic(fmt.Errorf("invalid config: %w", err))
	}
	return &cfg
}

func fetchConfigPath() string {
	var path string
	flag.StringVar(&path, "config", "./config.yml", "path to config file")
	flag.Parse()
	return path
}
