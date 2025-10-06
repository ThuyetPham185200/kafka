package main

import (
	"encoding/json"
	"fmt"
	kafkaclient "kafka/kafka-client"
	"os"
	"os/signal"
	"syscall"
	"time"
)

type UserMessage struct {
	ID      int    `json:"id"`
	Name    string `json:"name"`
	Message string `json:"message"`
}

func main() {
	// --- Producer setup ---
	prodCfg := kafkaclient.KafkaProducerConfig{
		BootstrapServers: "localhost:19092",
		Topic:            "my-topic",
	}
	producer := &kafkaclient.KafkaProducer{}
	producer.Init(prodCfg)
	producer.Open()
	defer producer.Close()

	data := UserMessage{
		ID:      1,
		Name:    "Thuyet",
		Message: "Hello Kafka with JSON bytes!",
	}

	jsonBytes, err := json.Marshal(data)
	if err != nil {
		fmt.Println("JSON encode error:", err)
		return
	}

	if err := producer.SendMsg(jsonBytes); err != nil {
		fmt.Println("Send error:", err)
	} else {
		fmt.Println("Send user message is success!")
	}
	producer.Flush(1000)

	// --- Consumer setup ---
	consCfg := kafkaclient.KafkaConsumerConfig{
		BootstrapServers: "localhost:19092",
		Topic:            "my-topic",
		GroupID:          "group-1",
		AutoOffsetReset:  "earliest",
	}
	consumer := &kafkaclient.KafkaConsumer{}
	consumer.Init(consCfg)
	consumer.Open()
	defer consumer.Close()

	// --- Graceful shutdown channel ---
	sigchan := make(chan os.Signal, 1)
	signal.Notify(sigchan, syscall.SIGINT, syscall.SIGTERM)

	fmt.Println("Consumer started. Press Ctrl+C to stop.")

	for {
		select {
		case <-sigchan:
			fmt.Println("\nShutting down gracefully...")
			return

		default:
			dataBytes, err := consumer.ReadMsg(500 * time.Millisecond)
			if err != nil {
				fmt.Println("Consumer read error:", err)
				continue
			}
			if dataBytes == nil {
				continue // no new message
			}

			var msg UserMessage
			if err := json.Unmarshal(dataBytes, &msg); err != nil {
				fmt.Println("JSON decode error:", err)
				continue
			}

			fmt.Printf("Consumed JSON: %+v\n", msg)
		}
	}
}
