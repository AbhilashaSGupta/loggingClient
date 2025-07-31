package main

import (
	"fmt"
	"log"
	"loggingClient/logging_client"
	"loggingClient/models"
)

func main() {
	fmt.Println("start processing")

	config := models.LoggingClientConfig{
		BucketName: "my-log-bucket",
		BufferSize: 500,
		ChunkSize:  5 * 1024 * 1024, // 5MB
		Region:     "us-west-2",
	}

	client, err := logging_client.NewLoggingClient(config)
	if err != nil {
		log.Fatalf("Failed to create logging client: %v", err)
	}
	defer func(client *models.LoggingClient) {
		err := client.Close()
		if err != nil {
			fmt.Println("error closing the logging client")
		}
	}(client)

	fmt.Println("finished processing")
}
