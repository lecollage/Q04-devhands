// monitoring_consumer.go

package main

import (
	"encoding/json"
	"fmt"
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
	Amount    float64 `json:"amount"`
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

	consumer, err := kafka.NewConsumer(&kafka.ConfigMap{
		"bootstrap.servers": config.BootstrapServers,
		"group.id":          "monitoring-group",
		"auto.offset.reset": "earliest",
	})

	if err != nil {
		panic(err)
	}

	consumer.Subscribe("user-events", nil)

	purchaseCount := 0
	totalAmount := 0.0

	startTime := time.Now()

	for {
		msg, err := consumer.ReadMessage(-1)
		if err != nil {
			if kafkaError, ok := err.(kafka.Error); ok && kafkaError.Code() == kafka.ErrPartitionEOF {
				fmt.Printf("End of partition reached %v\n", msg)
			} else {
				fmt.Printf("Error consuming message: %v\n", err)
			}
			continue
		}

		var event Event
		err = json.Unmarshal(msg.Value, &event)
		if err != nil {
			fmt.Printf("Error unmarshaling message: %v\n", err)
			continue
		}

		if event.EventType == "purchase" {
			purchaseCount++
			totalAmount += event.Amount
		}

		if time.Since(startTime).Seconds() > 15 {
			fmt.Printf("Purchases in last 15 seconds: %d\n", purchaseCount)
			fmt.Printf("Total amount in last 15 seconds: $%.2f\n", totalAmount)
			startTime = time.Now()
			purchaseCount = 0
			totalAmount = 0.0
		}
	}
}
