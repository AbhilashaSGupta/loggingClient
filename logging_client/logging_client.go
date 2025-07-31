package logging_client

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/config"
	_ "github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"log"
	"strings"
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
	MaxRetries        int
	KeyPrefix         string
	FlushInterval     time.Duration
}

type LoggingClient struct {
	Config      LoggingClientConfig
	S3Client    *s3.Client
	Buffer      []LogEntry
	HeartbeatMu sync.RWMutex
	BufferMu    sync.RWMutex
	isHealthy   bool
	Ctx         context.Context
	Cancel      context.CancelFunc // Store the cancel function
	// async heartbeat in background
	Wg        sync.WaitGroup
	flushChan chan interface{}
}

func NewLoggingClient(loggingClientConfig LoggingClientConfig) (*LoggingClient, error) {
	// Load AWS configuration
	cfg, err := config.LoadDefaultConfig(context.TODO(), config.WithRegion(loggingClientConfig.Region))
	if err != nil {
		return nil, fmt.Errorf("failed to load AWS config: %w", err)
	}

	s3Client := s3.NewFromConfig(cfg)

	ctx, cancel := context.WithCancel(context.Background())
	// Don't defer cancel here - store it in the struct instead else heartbeat does not work as expected.

	client := &LoggingClient{
		Config:    loggingClientConfig,
		S3Client:  s3Client,
		Buffer:    make([]LogEntry, 0, loggingClientConfig.BufferSize),
		isHealthy: false,
		Ctx:       ctx,
		Cancel:    cancel,
	}
	// Start background goroutine to heartbeat and flush
	client.Wg.Add(1)
	go client.heartbeatLoop()

	// Initial health check
	client.checkS3Health()

	return client, nil
}

// Log adds a log entry to the buffer
func (lc *LoggingClient) Log(level, message string, metadata map[string]interface{}) {
	entry := LogEntry{
		Timestamp: time.Now().UTC(),
		Level:     level,
		Message:   message,
		Metadata:  metadata,
	}

	lc.BufferMu.Lock()
	defer lc.BufferMu.Unlock()

	lc.Buffer = append(lc.Buffer, entry)

	// Trigger flush if buffer is full
	if len(lc.Buffer) >= lc.Config.BufferSize {
		select {
		case lc.flushChan <- struct{}{}:
		default:
			// Channel is full, flush is already pending
		}
	}
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

// IsHealthy heart beat functionality
func (lc *LoggingClient) IsHealthy() bool {
	lc.HeartbeatMu.RLock()
	defer lc.HeartbeatMu.RUnlock()
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
	// Create a timeout context for the health check
	ctx, cancel := context.WithTimeout(lc.Ctx, 10*time.Second)
	defer cancel()

	// Try to head the bucket to check connectivity as per aws docs
	_, err := lc.S3Client.HeadBucket(ctx, &s3.HeadBucketInput{
		Bucket: aws.String(lc.Config.BucketName),
	})

	lc.HeartbeatMu.Lock()
	defer lc.HeartbeatMu.Unlock()

	if err != nil {
		lc.isHealthy = false
		fmt.Printf("S3 health check failed: %v\n", err)
	} else {
		lc.isHealthy = true
		fmt.Println("S3 health check passed")
		lc.flushBuffer()
	}

}

// flushBuffer flushes the current buffer to S3
func (lc *LoggingClient) flushBuffer() {
	lc.BufferMu.Lock()
	if len(lc.Buffer) == 0 {
		lc.BufferMu.Unlock()
		return
	}

	// Copy buffer and clear it
	entries := make([]LogEntry, len(lc.Buffer))
	copy(entries, lc.Buffer)
	lc.Buffer = lc.Buffer[:0]
	lc.BufferMu.Unlock()

	// Convert entries to JSON
	var jsonData bytes.Buffer
	encoder := json.NewEncoder(&jsonData)
	for _, entry := range entries {
		if err := encoder.Encode(entry); err != nil {
			log.Printf("Failed to encode log entry: %v", err)
			continue
		}
	}

	if jsonData.Len() == 0 {
		return
	}

	// Generate S3 key
	const layout = "2006/01/02/15"
	timestamp := time.Now().UTC().Format(layout)
	key := fmt.Sprintf("%s/logs-%s-%d.jsonl",
		strings.TrimSuffix(lc.Config.KeyPrefix, "/"),
		timestamp,
		time.Now().UnixNano())

	// Upload to S3 with multipart if data is large enough
	if err := lc.uploadToS3(key, jsonData.Bytes()); err != nil {
		log.Printf("Failed to upload logs to S3: %v", err)
		// Put the entries back in buffer for retry
		lc.BufferMu.Lock()
		lc.Buffer = append(entries, lc.Buffer...)
		lc.BufferMu.Unlock()
	} else {
		log.Printf("Successfully uploaded %d log entries to S3: %s", len(entries), key)
	}
}
