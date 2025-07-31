package logging_client

import (
	"context"
	"fmt"
	"github.com/aws/aws-sdk-go-v2/config"
	_ "github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"sync"
	"time"
)

// Single Log Entry
type LogEntry struct {
	Timestamp time.Time
	Level     string
	Message   string
	Metadata  map[string]interface{}
}

type LoggingClientConfig struct {
	//s3 bucket configs
	BucketName string
	Region     string
	ChunkSize  int64
	// from the caller
	BufferSize        int
	HeartbeatInterval time.Duration
}

type LoggingClient struct {
	Config    LoggingClientConfig
	S3Client  *s3.Client
	Buffer    []LogEntry
	Mu        sync.RWMutex
	isHealthy bool
	Ctx       context.Context
	Cancel    context.CancelFunc // Store the cancel function
	// async heartbeat in background
	Wg sync.WaitGroup
}

func NewLoggingClient(loggingClientConfig LoggingClientConfig) (*LoggingClient, error) {
	// Load AWS configuration
	cfg, err := config.LoadDefaultConfig(context.TODO(), config.WithRegion(loggingClientConfig.Region))
	if err != nil {
		return nil, fmt.Errorf("failed to load AWS config: %w", err)
	}

	s3Client := s3.NewFromConfig(cfg)

	ctx, cancel := context.WithCancel(context.Background())
	// Don't defer cancel here - store it in the struct instead

	client := &LoggingClient{
		Config:    loggingClientConfig,
		S3Client:  s3Client,
		Buffer:    make([]LogEntry, 0, loggingClientConfig.BufferSize),
		isHealthy: false,
		Ctx:       ctx,
		Cancel:    cancel,
	}
	// Start background goroutine
	client.Wg.Add(1)
	go client.heartbeatLoop()

	// Initial health check
	client.checkS3Health()

	return client, nil
}

func (lc *LoggingClient) Close() error {
	// TODO : flush buffer prior to close
	// Wait for goroutines to finish
	lc.Wg.Wait()

	if lc.Cancel != nil {
		lc.Cancel()
	}

	return nil
}

// heart beat functionality
func (lc *LoggingClient) IsHealthy() bool {
	lc.Mu.RLock()
	defer lc.Mu.RUnlock()
	return lc.isHealthy
}

// heartbeatLoop runs the periodic health check
func (lc *LoggingClient) heartbeatLoop() {
	defer lc.Wg.Done()
	ticker := time.NewTicker(lc.Config.HeartbeatInterval)
	defer ticker.Stop()

	fmt.Println("Heartbeat loop started")

	for {
		select {

		case <-lc.Ctx.Done():
			fmt.Println("Heartbeat loop stopping - context cancelled")
			return
		case <-ticker.C:
			lc.checkS3Health()
		}
	}
}

func (lc *LoggingClient) checkS3Health() {
	// check s3 status here
	fmt.Println("heartbeat check")

}
