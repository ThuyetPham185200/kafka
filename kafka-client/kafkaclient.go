package kafkaclient

import (
	"fmt"
	"time"

	"github.com/confluentinc/confluent-kafka-go/kafka"
)

// ===== Common Kafka Interface =====
type KafkaInterface interface {
	Init(config any) error // `any` allows ProducerConfig or ConsumerConfig
	Open() error
	Close() error
}

// ===== Config Structs =====
type KafkaProducerConfig struct {
	BootstrapServers string
	Topic            string
}

type KafkaConsumerConfig struct {
	BootstrapServers string
	Topic            string
	GroupID          string
	AutoOffsetReset  string
}

// ===== Producer Implementation =====
type KafkaProducer struct {
	producer *kafka.Producer
	config   KafkaProducerConfig
}

func (p *KafkaProducer) Init(config any) error {
	cfg, ok := config.(KafkaProducerConfig)
	if !ok {
		return fmt.Errorf("invalid config type for producer")
	}
	if cfg.BootstrapServers == "" || cfg.Topic == "" {
		return fmt.Errorf("missing required producer config fields")
	}
	p.config = cfg
	return nil
}

func (p *KafkaProducer) Open() error {
	prod, err := kafka.NewProducer(&kafka.ConfigMap{
		"bootstrap.servers": p.config.BootstrapServers,
	})
	if err != nil {
		return err
	}
	p.producer = prod
	fmt.Println("Kafka producer opened")
	return nil
}

func (p *KafkaProducer) Close() error {
	if p.producer != nil {
		p.producer.Flush(15 * 1000)
		p.producer.Close()
		fmt.Println("Kafka producer closed")
	}
	return nil
}

func (p *KafkaProducer) Flush(milisecond int) {
	p.producer.Flush(milisecond)
}

// Send raw []byte message
func (p *KafkaProducer) SendMsg(data []byte) error {
	err := p.producer.Produce(&kafka.Message{
		TopicPartition: kafka.TopicPartition{Topic: &p.config.Topic, Partition: kafka.PartitionAny},
		Value:          data,
	}, nil)

	if err != nil {
		return fmt.Errorf("failed to send message: %w", err)
	}

	return nil
}

// ===== Consumer Implementation =====
type KafkaConsumer struct {
	consumer *kafka.Consumer
	config   KafkaConsumerConfig
}

func (c *KafkaConsumer) Init(config any) error {
	cfg, ok := config.(KafkaConsumerConfig)
	if !ok {
		return fmt.Errorf("invalid config type for consumer")
	}
	if cfg.BootstrapServers == "" || cfg.Topic == "" || cfg.GroupID == "" {
		return fmt.Errorf("missing required consumer config fields")
	}
	if cfg.AutoOffsetReset == "" {
		cfg.AutoOffsetReset = "earliest"
	}
	c.config = cfg
	return nil
}

func (c *KafkaConsumer) Open() error {
	cons, err := kafka.NewConsumer(&kafka.ConfigMap{
		"bootstrap.servers": c.config.BootstrapServers,
		"group.id":          c.config.GroupID,
		"auto.offset.reset": c.config.AutoOffsetReset,
	})
	if err != nil {
		return err
	}

	if err := cons.SubscribeTopics([]string{c.config.Topic}, nil); err != nil {
		return err
	}

	c.consumer = cons
	fmt.Printf("Kafka consumer opened (group=%s) subscribed to: %s\n",
		c.config.GroupID, c.config.Topic)
	return nil
}

func (c *KafkaConsumer) Close() error {
	if c.consumer != nil {
		c.consumer.Close()
		fmt.Println("Kafka consumer closed")
	}
	return nil
}

func (c *KafkaConsumer) ReadMsg(timeout time.Duration) ([]byte, error) {
	if c.consumer == nil {
		return nil, fmt.Errorf("consumer not initialized or opened")
	}

	msg, err := c.consumer.ReadMessage(timeout)
	if err != nil {
		// Handle timeout error gracefully
		if kafkaErr, ok := err.(kafka.Error); ok && kafkaErr.Code() == kafka.ErrTimedOut {
			return nil, nil
		}
		return nil, fmt.Errorf("read message error: %w", err)
	}

	fmt.Printf("Received from partition %d offset %d: %s\n",
		msg.TopicPartition.Partition, msg.TopicPartition.Offset, string(msg.Value))

	return msg.Value, nil
}
