package main

import (
	"context"
	"fmt"
	"os"
	"os/signal"
	"sync"
	"syscall"
	"time"

	"github.com/confluentinc/confluent-kafka-go/kafka"
)

// Kafka configuration
var (
	bootstrapServers  = "localhost:19092" // Change to your broker
	topic             = "my-topic"
	numPartitions     = 3
	replicationFactor = 1
	consumerGroupID   = "my-consumer-group"
)

// Create topic
func CreateTopic() error {
	adminClient, err := kafka.NewAdminClient(&kafka.ConfigMap{
		"bootstrap.servers": bootstrapServers,
	})
	if err != nil {
		return err
	}
	defer adminClient.Close()

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	results, err := adminClient.CreateTopics(ctx, []kafka.TopicSpecification{
		{
			Topic:             topic,
			NumPartitions:     numPartitions,
			ReplicationFactor: replicationFactor,
		},
	})
	if err != nil {
		return err
	}

	for _, r := range results {
		if r.Error.Code() != kafka.ErrNoError && r.Error.Code() != kafka.ErrTopicAlreadyExists {
			return r.Error
		}
	}
	fmt.Println("Topic ready:", topic)
	return nil
}

// Producer goroutine
func RunProducer(numMessages int, wg *sync.WaitGroup) {
	defer wg.Done()

	producer, err := kafka.NewProducer(&kafka.ConfigMap{
		"bootstrap.servers": bootstrapServers,
	})
	if err != nil {
		fmt.Println("Producer error:", err)
		return
	}
	defer producer.Close()

	for i := 0; i < numMessages; i++ {
		msg := &kafka.Message{
			TopicPartition: kafka.TopicPartition{Topic: &topic, Partition: kafka.PartitionAny},
			Value:          []byte(fmt.Sprintf("Message %d", i)),
		}
		if err := producer.Produce(msg, nil); err != nil {
			fmt.Println("Produce error:", err)
			return
		}
		fmt.Printf("Produced: %s\n", msg.Value)
		time.Sleep(200 * time.Millisecond) // slow down so consumers can show results
	}

	producer.Flush(5000)
}

// Consumer goroutine
func RunConsumer(id int, wg *sync.WaitGroup) {
	defer wg.Done()

	consumer, err := kafka.NewConsumer(&kafka.ConfigMap{
		"bootstrap.servers": bootstrapServers,
		"group.id":          consumerGroupID,
		"auto.offset.reset": "earliest",
	})
	if err != nil {
		fmt.Println("Consumer error:", err)
		return
	}
	defer consumer.Close()

	err = consumer.SubscribeTopics([]string{topic}, nil)
	if err != nil {
		fmt.Println("Subscribe error:", err)
		return
	}

	fmt.Printf("Consumer %d started\n", id)

	for {
		msg, err := consumer.ReadMessage(5 * time.Second)
		if err != nil {
			if kafkaErr, ok := err.(kafka.Error); ok && kafkaErr.Code() == kafka.ErrTimedOut {
				continue
			}
			fmt.Printf("Consumer %d error: %v\n", id, err)
			return
		}
		fmt.Printf("Consumer %d received: %s from partition %d at offset %d\n",
			id, string(msg.Value), msg.TopicPartition.Partition, msg.TopicPartition.Offset)
	}
}

func main() {
	// Ensure topic exists
	_ = CreateTopic()

	var wg sync.WaitGroup

	// Start producer goroutine
	wg.Add(1)
	go RunProducer(30, &wg)

	// Start 3 consumers in goroutines
	for i := 1; i <= 3; i++ {
		wg.Add(1)
		go RunConsumer(i, &wg)
	}

	// Graceful shutdown
	sigchan := make(chan os.Signal, 1)
	signal.Notify(sigchan, syscall.SIGINT, syscall.SIGTERM)
	<-sigchan
	fmt.Println("Shutting down...")

	wg.Wait()
}
