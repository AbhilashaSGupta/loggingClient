package models

import (
	"context"
	"sync"
	"time"

	"github.com/aws/aws-sdk-go-v2/service/s3"
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
	BufferSize int
}

type LoggingClient struct {
	Config    LoggingClientConfig
	S3Client  *s3.Client
	Buffer    []LogEntry
	mu        sync.RWMutex
	IsHealthy bool
	Ctx       context.Context
}

func (lc *LoggingClient) Close() error {
	// flush buffer prior to close
	return nil
}

//
