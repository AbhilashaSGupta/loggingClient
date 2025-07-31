package logging_client

import (
	"context"
	"fmt"
	"github.com/aws/aws-sdk-go-v2/config"
	_ "github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"loggingClient/models"
)

func NewLoggingClient(loggingClientConfig models.LoggingClientConfig) (*models.LoggingClient, error) {
	// Load AWS configuration
	cfg, err := config.LoadDefaultConfig(context.TODO(), config.WithRegion(loggingClientConfig.Region))
	if err != nil {
		return nil, fmt.Errorf("failed to load AWS config: %w", err)
	}

	s3Client := s3.NewFromConfig(cfg)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	client := &models.LoggingClient{
		Config:    loggingClientConfig,
		S3Client:  s3Client,
		Buffer:    make([]models.LogEntry, 0, loggingClientConfig.BufferSize),
		IsHealthy: false,
		Ctx:       ctx,
	}
	return client, nil
}
