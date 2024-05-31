package main

import (
	"fmt"
	"time"

	"github.com/confluentinc/confluent-kafka-go/v2/kafka"
)

func main() {
	p, err := kafka.NewProducer(&kafka.ConfigMap{"bootstrap.servers": "localhost:9092"})
	if err != nil {
		panic(err)
	}
	defer p.Close()

	fmt.Printf("Created Producer %v\n", p)

	// Delivery report handler for produced messages
	go func() {
		for e := range p.Events() {
			switch ev := e.(type) {
			case *kafka.Message:
				if ev.TopicPartition.Error != nil {
					fmt.Printf("Delivery failed: %v\n", ev.TopicPartition)
				} else {
					m := ev
					fmt.Printf("[Now: %d] Delivered message (%s) to topic %s [%d] with offset %v at %d\n",
						time.Now().Unix(),
						string(m.Value), *m.TopicPartition.Topic, m.TopicPartition.Partition, m.TopicPartition.Offset, m.Timestamp.Unix())
				}
			case kafka.Error:
				fmt.Printf("Error: %v\n", ev)
			default:
				fmt.Printf("Ignored event: %s\n", ev)
			}
		}
	}()

	// Produce messages to topic (asynchronously)
	topic := "test-kafka-go"

	// messages := []string{"Welcome", "to", "the", "Confluent", "Kafka", "Golang", "client"}
	messages := []string{"sample", "sample2", "sample3"}
	for i, word := range messages {
		fmt.Printf("producing msg(%s) at %d\n", word, time.Now().Add(-(20 * 365 * 24 * time.Hour)).Unix())
		if err := p.Produce(&kafka.Message{
			TopicPartition: kafka.TopicPartition{Topic: &topic, Partition: kafka.PartitionAny},
			Value:          []byte(word),
			Headers:        []kafka.Header{{Key: "myTestHeader", Value: []byte("header values are binary")}},
			Timestamp:      time.Now().Add(-(20 * 365 * 24 * time.Hour)),
			TimestampType:  kafka.TimestampCreateTime,
		}, nil); err != nil {
			fmt.Printf("Failed to produce message: %v\n", err)
		}

		time.Sleep(time.Duration(i+1) * time.Second)
	}
	fmt.Println("produced all messages")

	// Wait for message deliveries before shutting down
	p.Flush(1000)

	fmt.Println("sleeping here!")
	time.Sleep(1 * time.Minute)
}
