package main

import (
	"fmt"
	"log"
	"loggingClient/logging_client"
	"time"
)

func main() {
	fmt.Println("start processing")

	config := logging_client.LoggingClientConfig{
		BucketName:        "my-log-bucket",
		BufferSize:        500,
		ChunkSize:         5 * 1024 * 1024, // 5MB
		Region:            "us-west-2",
		HeartbeatInterval: 1 * time.Second,
	}

	lc, err := logging_client.NewLoggingClient(config)
	if err != nil {
		log.Fatalf("Failed to create logging client: %v", err)
	}
	defer func(client *logging_client.LoggingClient) {
		err := client.Close()
		if err != nil {
			fmt.Println("error closing the logging client")
		}
	}(lc)
	time.Sleep(1 * time.Minute)
	fmt.Println("finished processing")
}
