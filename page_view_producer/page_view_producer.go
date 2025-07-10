package main

import (
	"encoding/json"
	"fmt"
	"math/rand"
	"os"
	"time"

	"hash/fnv"

	"github.com/confluentinc/confluent-kafka-go/kafka"
	"gopkg.in/yaml.v2"
)

type Config struct {
	BootstrapServers string `yaml:"bootstrap.servers"`
}

type Event struct {
	EventType string  `json:"event_type"`
	UserID    int     `json:"user_id"`
	PageID    int     `json:"page_id"`
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

func customPartitioner(topic string, key []byte, keyAsString string, value []byte, valueLength int, availablePartitions []int) int {
	hasher := fnv.New32a()
	hasher.Write(key)
	hash := hasher.Sum32()
	return availablePartitions[int(hash)%len(availablePartitions)]
}

func getNumberOfPartitions(adminClient *kafka.AdminClient, topic string) (int, error) {
	metadata, err := adminClient.GetMetadata(&topic, false, 10000)
	if err != nil {
		return 0, err
	}
	topicMetadata, exists := metadata.Topics[topic]
	if !exists {
		return 0, fmt.Errorf("topic %s not found", topic)
	}
	return len(topicMetadata.Partitions), nil
}

func main() {
	config, err := loadConfig()
	if err != nil {
		panic(err)
	}

	producer, err := kafka.NewProducer(&kafka.ConfigMap{
		"bootstrap.servers": config.BootstrapServers,
		"acks":              "1",
	})

	if err != nil {
		panic(err)
	}

	defer producer.Close()

	topic := "user-events"
	deliveryChan := make(chan kafka.Event)

	adminClient, err := kafka.NewAdminClient(&kafka.ConfigMap{
		"bootstrap.servers": config.BootstrapServers,
	})
	if err != nil {
		panic(err)
	}
	partitions := 0
	for partitions == 0 {
		partitions, err = getNumberOfPartitions(adminClient, topic)
		if err != nil {
			panic(err)
		}
		time.Sleep(1000 * time.Millisecond)
	}
	fmt.Printf("Topic %s has %d partitions\n", topic, partitions)

	for {
		userID := rand.Intn(1000) + 1
		event := Event{
			EventType: "page_view",
			UserID:    userID,
			PageID:    rand.Intn(100) + 1,
			Timestamp: float64(time.Now().Unix()),
		}
		eventBytes, _ := json.Marshal(event)
		message := &kafka.Message{
			TopicPartition: kafka.TopicPartition{
				Topic: &topic,
				// Partition: kafka.PartitionAny,
				Partition: int32(userID % partitions),
			},
			Key:   []byte(fmt.Sprint(userID)),
			Value: eventBytes,
		}

		err := producer.Produce(message, deliveryChan)
		if err != nil {
			fmt.Errorf("failed to produce message: %w", err)
		}
		producer.Flush(0)

		e := <-deliveryChan
		m := e.(*kafka.Message)

		// Check for delivery errors
		if m.TopicPartition.Error != nil {
			fmt.Errorf("delivery failed: %s", m.TopicPartition.Error)
		} else {
			fmt.Printf("Delivered message to %v, Event: %s\n", m.TopicPartition, string(eventBytes))
		}

		time.Sleep(300*time.Millisecond + time.Duration(rand.Intn(500))*time.Millisecond)
	}
}
