package main

import (
	"fmt"
	"github.com/AbhilashaSGupta/loggingClient/logging_client" // Updated import path
	"loggingClient/clients"
	"time"
)

func main() {
	fmt.Println("start processing")

	config := logging_client.LoggingClientConfig{
		BucketName:        "my-log-bucket",
		KeyPrefix:         "my-application-name",
		BufferSize:        500,
		ChunkSize:         5 * 1024 * 1024, // 5MB
		Region:            "us-west-2",
		HeartbeatInterval: 5 * time.Minute,
	}

	lc, err := clients.NewLoggingClient(config)
	if err != nil {
		fmt.Println("Failed to create logging client: %v", err)
	}
	defer func(client *clients.LoggingClient) {
		err := client.Close()
		if err != nil {
			fmt.Println("error closing the logging client")
		}
	}(lc)
	fmt.Println("finished processing")
}
