package main

import (
	"context"
	"fmt"
	"os"

	"github.com/confluentinc/confluent-kafka-go/v2/kafka"
)

func main() {
	adCli, err := kafka.NewAdminClient(&kafka.ConfigMap{"bootstrap.servers": "localhost:9092"})
	if err != nil {
		fmt.Printf("Failed to create Admin client: %s\n", err)
		os.Exit(1)
	}
	defer adCli.Close()

	topic := "test-kafka-go"

	fmt.Println("calling topic describe")
	result, err := adCli.DescribeTopics(context.Background(), kafka.NewTopicCollectionOfTopicNames([]string{topic}), nil)
	if err != nil {
		fmt.Printf("Failed to describe topic: %s\n", err)
		os.Exit(1)
	}
	fmt.Println(result.TopicDescriptions)
}
