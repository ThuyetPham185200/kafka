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
type KafkaConfig struct {
	BootstrapServers string
	Topic            string
	ExtraConfig      map[string]string
}

// ===== Producer Implementation =====
type KafkaProducer struct {
	producer *kafka.Producer
	config   KafkaConfig
}

func (p *KafkaProducer) Init(config any) error {
	cfg, ok := config.(KafkaConfig)
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
	configMap := &kafka.ConfigMap{
		"bootstrap.servers": p.config.BootstrapServers,
	}

	// Merge extra configs if provided
	fmt.Printf("Kafka producer configuration:\n")
	for key, value := range p.config.ExtraConfig {
		if err := configMap.SetKey(key, value); err != nil {
			fmt.Printf("Warning: failed to set extra config %s=%s: %v\n", key, value, err)
		} else {
			fmt.Printf("     %s : %s\n", key, value)
		}
	}

	prod, err := kafka.NewProducer(configMap)
	if err != nil {
		return fmt.Errorf("failed to create producer: %w", err)
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

// SendMsg sends a raw []byte message to Kafka and waits for delivery confirmation
func (p *KafkaProducer) Push(data []byte) error {
	if p.producer == nil {
		return fmt.Errorf("producer not initialized or opened")
	}

	deliveryChan := make(chan kafka.Event, 1)

	err := p.producer.Produce(&kafka.Message{
		TopicPartition: kafka.TopicPartition{Topic: &p.config.Topic, Partition: kafka.PartitionAny},
		Value:          data,
	}, deliveryChan)

	if err != nil {
		close(deliveryChan)
		return fmt.Errorf("failed to send message: %w", err)
	}

	// Wait for delivery report
	e := <-deliveryChan
	m := e.(*kafka.Message)
	close(deliveryChan)

	if m.TopicPartition.Error != nil {
		return fmt.Errorf("delivery failed: %v", m.TopicPartition.Error)
	}

	fmt.Printf("Delivered message to topic %s [%d] at offset %v\n",
		*m.TopicPartition.Topic, m.TopicPartition.Partition, m.TopicPartition.Offset)

	return nil
}

// ===== Consumer Implementation =====
type KafkaConsumer struct {
	consumer *kafka.Consumer
	config   KafkaConfig
}

func (c *KafkaConsumer) Init(config any) error {
	cfg, ok := config.(KafkaConfig)
	if !ok {
		return fmt.Errorf("invalid config type for consumer")
	}
	if cfg.BootstrapServers == "" || cfg.Topic == "" {
		return fmt.Errorf("missing required consumer config fields")
	}
	c.config = cfg
	return nil
}

func (c *KafkaConsumer) Open() error {

	configMap := &kafka.ConfigMap{
		"bootstrap.servers": c.config.BootstrapServers,
	}

	fmt.Printf("Kafka consumer configuration:\n")
	for key, value := range c.config.ExtraConfig {
		if err := configMap.SetKey(key, value); err != nil {
			fmt.Printf("Warning: failed to set extra config %s=%s: %v\n", key, value, err)
		} else {
			fmt.Printf("     %s : %s\n", key, value)
		}
	}

	cons, err := kafka.NewConsumer(configMap)
	if err != nil {
		return err
	}

	if err := cons.SubscribeTopics([]string{c.config.Topic}, nil); err != nil {
		return err
	}

	c.consumer = cons
	fmt.Printf("Kafka consumer subscribed to: %s\n", c.config.Topic)
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

func (c *KafkaConsumer) Poll(timeout time.Duration) ([]byte, error) {
	if c.consumer == nil {
		return nil, fmt.Errorf("consumer not initialized or opened")
	}

	ev := c.consumer.Poll(int(timeout.Milliseconds()))
	if ev == nil {
		return nil, nil // no new messages
	}

	switch e := ev.(type) {
	case *kafka.Message:
		return e.Value, nil

	case kafka.Error:
		return nil, fmt.Errorf("kafka error: %v", e)

	default:
		// Ignore other events like stats, partition assignments, etc.
		return nil, nil
	}
}
