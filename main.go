package main

import (
	"encoding/json"
	"fmt"
	kafkaclient "kafka/kafka-client"
	"os"
	"os/signal"
	"sync"
	"syscall"
	"time"
)

type UserMessage struct {
	ID      int    `json:"id"`
	Name    string `json:"name"`
	Message string `json:"message"`
	Time    int64  `json:"time"`
}

func main() {
	prodCfg := kafkaclient.KafkaConfig{
		BootstrapServers: "localhost:19092",
		Topic:            "my-topic",
	}
	producer := &kafkaclient.KafkaProducer{}
	producer.Init(prodCfg)
	producer.Open()
	defer producer.Close()

	consCfg := kafkaclient.KafkaConfig{
		BootstrapServers: "localhost:19092",
		Topic:            "my-topic",
		ExtraConfig: map[string]string{
			"group.id":          "group-1",
			"auto.offset.reset": "earliest",
		},
	}
	consumer := &kafkaclient.KafkaConsumer{}
	consumer.Init(consCfg)
	consumer.Open()
	defer consumer.Close()

	var wg sync.WaitGroup
	stop := make(chan struct{})

	// --- Producer goroutine ---
	wg.Add(1)
	go func() {
		defer wg.Done()
		ticker := time.NewTicker(10 * time.Millisecond) // 100 Hz
		defer ticker.Stop()
		id := 0
		for {
			select {
			case <-stop:
				return
			case <-ticker.C:
				msg := UserMessage{
					ID:      id,
					Name:    "Thuyet",
					Message: "Hello from producer",
					Time:    time.Now().UnixNano(),
				}
				data, _ := json.Marshal(msg)
				if err := producer.Push(data); err != nil {
					fmt.Println("Push error:", err)
				}
				id++
			}
		}
	}()

	// --- Consumer goroutine ---
	wg.Add(1)
	go func() {
		defer wg.Done()
		for {
			select {
			case <-stop:
				return
			default:
				data, err := consumer.Poll(100)
				if err != nil || data == nil {
					continue
				}
				var msg UserMessage
				if err := json.Unmarshal(data, &msg); err != nil {
					fmt.Println("Decode error:", err)
					continue
				}
				fmt.Printf("📥 Consumed: %+v\n", msg)
			}
		}
	}()

	// Graceful shutdown
	sig := make(chan os.Signal, 1)
	signal.Notify(sig, syscall.SIGINT, syscall.SIGTERM)
	<-sig
	close(stop)
	wg.Wait()
	fmt.Println("Shutdown complete.")
}
