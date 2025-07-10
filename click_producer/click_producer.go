package main

import (
	"encoding/json"
	"fmt"
	"math/rand"
	"os"
	"time"

	"github.com/confluentinc/confluent-kafka-go/kafka"
	"gopkg.in/yaml.v2"
)

type Config struct {
	BootstrapServers string `yaml:"bootstrap.servers"`
}

type Event struct {
	EventType string  `json:"event_type"`
	UserID    int     `json:"user_id"`
	ItemID    int     `json:"item_id"`
	Timestamp float64 `json:"timestamp"`
}

func loadConfig() (*Config, error) {
	config := &Config{}
	data, err := os.ReadFile("config.yaml")
	if err != nil {
		return nil, err
	}
	err = yaml.Unmarshal(data, config)
	if err != nil {
		return nil, err
	}
	return config, nil
}

func main() {
	config, err := loadConfig()
	if err != nil {
		panic(err)
	}

	producer, err := kafka.NewProducer(&kafka.ConfigMap{
		"bootstrap.servers": config.BootstrapServers,
		"acks":              "0",
	})

	if err != nil {
		panic(err)
	}

	defer producer.Close()

	topic := "user-events"
	deliveryChan := make(chan kafka.Event)

	for {
		userID := rand.Intn(1000) + 1
		event := Event{
			EventType: "click",
			UserID:    userID,
			ItemID:    rand.Intn(1000) + 1,
			Timestamp: float64(time.Now().Unix()),
		}
		eventBytes, _ := json.Marshal(event)
		message := &kafka.Message{
			TopicPartition: kafka.TopicPartition{
				Topic:     &topic,
				Partition: kafka.PartitionAny,
			},
			Key:   []byte(fmt.Sprint(userID)),
			Value: eventBytes,
		}

		err := producer.Produce(message, deliveryChan)
		if err != nil {
			_ = fmt.Errorf("failed to produce message: %w", err)
		}
		producer.Flush(0)

		e := <-deliveryChan
		m := e.(*kafka.Message)

		// Check for delivery errors
		if m.TopicPartition.Error != nil {
			_ = fmt.Errorf("delivery failed: %s", m.TopicPartition.Error)
		} else {
			fmt.Printf("Delivered message to %v, Event: %s\n", m.TopicPartition, string(eventBytes))
		}

		time.Sleep(300*time.Millisecond + time.Duration(rand.Intn(333))*time.Millisecond)
	}
}
