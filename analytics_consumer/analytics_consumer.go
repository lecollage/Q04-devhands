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
	EventType string `json:"event_type"`
	UserID    int    `json:"user_id"`
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
		"group.id":          "analytics-group",
		"auto.offset.reset": "earliest",
	})

	if err != nil {
		panic(err)
	}

	consumer.Subscribe("user-events", nil)

	eventCounts := map[string]int{
		"page_view": 0,
		"click":     0,
		"purchase":  0,
	}
	distinctUsers := make(map[int]bool)

	printAggregates := func() {
		fmt.Println("Event Counts:")
		for eventType, count := range eventCounts {
			fmt.Printf("  %s: %d\n", eventType, count)
		}
		fmt.Printf("Distinct Users: %d\n", len(distinctUsers))
	}

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

		eventCounts[event.EventType]++
		distinctUsers[event.UserID] = true

		if time.Since(startTime).Seconds() > 3 {
			printAggregates()
			startTime = time.Now()
			for key := range eventCounts {
				eventCounts[key] = 0
			}
			distinctUsers = make(map[int]bool)
		}
	}
}
