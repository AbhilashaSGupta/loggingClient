package main

import (
	"fmt"
	"log"
	"loggingClient/clients"
	"time"
)

func main() {
	fmt.Println("start processing")

	config := clients.LoggingClientConfig{
		BucketName:        "my-log-bucket",
		BufferSize:        500,
		ChunkSize:         5 * 1024 * 1024, // 5MB
		Region:            "us-west-2",
		HeartbeatInterval: 1 * time.Second,
	}

	lc, err := clients.NewLoggingClient(config)
	if err != nil {
		log.Fatalf("Failed to create logging client: %v", err)
	}
	defer func(client *clients.LoggingClient) {
		err := client.Close()
		if err != nil {
			fmt.Println("error closing the logging client")
		}
	}(lc)
	time.Sleep(1 * time.Minute)
	fmt.Println("finished processing")
}
