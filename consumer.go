package main

import (
	"fmt"
	"os"
	"time"

	"github.com/confluentinc/confluent-kafka-go/v2/kafka"
)

func main() {
	cons, err := kafka.NewConsumer(&kafka.ConfigMap{
		"bootstrap.servers": "localhost:9092",
		"group.id":          "consumer-group-1",
		// "broker.address.family": "v4",
		// "session.timeout.ms":       6000,
		// "auto.offset.reset":        "earliest",
		"auto.offset.reset":        "latest",
		"enable.auto.offset.store": false,
	})
	if err != nil {
		panic(err)
	}
	defer cons.Close()

	topic := "test-kafka-go"
	if err := cons.SubscribeTopics([]string{topic}, nil); err != nil {
		panic("failed to subscribe topic")
	}

	fmt.Println("start listening for events")

	cons.Assign([]kafka.TopicPartition{
		{
			Topic:     &topic,
			Partition: 0,
		},
	})
	part, err := cons.Assignment()
	fmt.Println(part, err)

	for {
		ev := cons.Poll(1000)
		if ev == nil {
			fmt.Println("event is nil", ev)
			continue
		}

		switch e := ev.(type) {
		case *kafka.Message:
			// Process the message received.
			m := e
			fmt.Printf("[Now: %d] Received message (%s) from topic %s [%d] with offset %v at %d\n",
				time.Now().Unix(),
				string(m.Value), *m.TopicPartition.Topic, m.TopicPartition.Partition, m.TopicPartition.Offset, m.Timestamp.Unix())
			if e.Headers != nil {
				fmt.Printf("Headers: %v\n", e.Headers)
			}

			_, err := cons.StoreMessage(e)
			if err != nil {
				fmt.Fprintf(os.Stderr, "%% Error storing offset after message %s:\n",
					e.TopicPartition)
			}
		case kafka.Error:
			fmt.Fprintf(os.Stderr, "%% Error: %v: %v\n", e.Code(), e)
			if e.Code() == kafka.ErrAllBrokersDown {
				break
			}
		default:
			fmt.Printf("Ignored %v\n", e)
		}
	}
}
