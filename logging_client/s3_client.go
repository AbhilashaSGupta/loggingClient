package logging_client

import (
	"bytes"
	"context"
	"fmt"
	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/aws/aws-sdk-go-v2/service/s3/types"
	"log"
	"time"
)

// S3ClientConfig holds configuration for the S3 client
type S3ClientConfig struct {
	BucketName string
	Region     string
	ChunkSize  int64 // Size of each chunk in multipart upload (minimum 5MB)
	MaxRetries int   // Maximum retry attempts for S3 operations
}

// S3Client handles all S3 operations
type S3Client struct {
	config   S3ClientConfig
	s3Client *s3.Client
}

// NewS3Client creates a new S3 client instance
func NewS3Client(s3config S3ClientConfig) (*S3Client, error) {
	// Load AWS configuration
	cfg, err := config.LoadDefaultConfig(context.TODO(), config.WithRegion(s3config.Region))
	if err != nil {
		return nil, fmt.Errorf("failed to load AWS config: %w", err)
	}

	s3Client := s3.NewFromConfig(cfg)

	return &S3Client{
		config:   s3config,
		s3Client: s3Client,
	}, nil
}

// CheckHealth performs a health check against S3
func (sc *S3Client) CheckHealth(ctx context.Context) error {
	// Create a timeout context for the health check
	healthCtx, cancel := context.WithTimeout(ctx, 10*time.Second)
	defer cancel()

	// Try to head the bucket to check connectivity
	_, err := sc.s3Client.HeadBucket(healthCtx, &s3.HeadBucketInput{
		Bucket: aws.String(sc.config.BucketName),
	})

	return err
}

// UploadData uploads data to S3 using the most appropriate method
func (sc *S3Client) UploadData(ctx context.Context, key string, data []byte) error {
	dataSize := int64(len(data))

	// Use simple put for small files
	if dataSize < sc.config.ChunkSize {
		return sc.simplePutObject(ctx, key, data)
	}

	// Use multipart upload for larger files
	return sc.multipartUpload(ctx, key, data)
}

// simplePutObject uploads data using a simple PUT operation
func (sc *S3Client) simplePutObject(ctx context.Context, key string, data []byte) error {
	putCtx, cancel := context.WithTimeout(ctx, 30*time.Second)
	defer cancel()

	for attempt := 0; attempt < sc.config.MaxRetries; attempt++ {
		_, err := sc.s3Client.PutObject(putCtx, &s3.PutObjectInput{
			Bucket:      aws.String(sc.config.BucketName),
			Key:         aws.String(key),
			Body:        bytes.NewReader(data),
			ContentType: aws.String("application/jsonl"),
		})

		if err == nil {
			return nil
		}

		log.Printf("Put object attempt %d failed: %v", attempt+1, err)
		if attempt < sc.config.MaxRetries-1 {
			time.Sleep(time.Duration(attempt+1) * time.Second)
		}
	}

	return fmt.Errorf("failed to put object after %d attempts", sc.config.MaxRetries)
}

// multipartUpload uploads data using S3 multipart upload
func (sc *S3Client) multipartUpload(ctx context.Context, key string, data []byte) error {
	uploadCtx, cancel := context.WithTimeout(ctx, 5*time.Minute)
	defer cancel()

	// Create multipart upload
	createResp, err := sc.s3Client.CreateMultipartUpload(uploadCtx, &s3.CreateMultipartUploadInput{
		Bucket:      aws.String(sc.config.BucketName),
		Key:         aws.String(key),
		ContentType: aws.String("application/jsonl"),
	})
	if err != nil {
		return fmt.Errorf("failed to create multipart upload: %w", err)
	}

	uploadID := createResp.UploadId
	var completedParts []types.CompletedPart

	// Upload parts
	dataLen := int64(len(data))
	partNumber := int32(1)

	for offset := int64(0); offset < dataLen; offset += sc.config.ChunkSize {
		end := offset + sc.config.ChunkSize
		if end > dataLen {
			end = dataLen
		}

		partData := data[offset:end]

		uploadResp, err := sc.uploadPart(uploadCtx, uploadID, key, partNumber, partData)
		if err != nil {
			// Abort multipart upload on error
			sc.abortMultipartUpload(uploadCtx, uploadID, key)
			return fmt.Errorf("failed to upload part %d: %w", partNumber, err)
		}

		completedParts = append(completedParts, types.CompletedPart{
			ETag:       uploadResp.ETag,
			PartNumber: aws.Int32(partNumber),
		})

		partNumber++
	}

	// Complete multipart upload
	_, err = sc.s3Client.CompleteMultipartUpload(uploadCtx, &s3.CompleteMultipartUploadInput{
		Bucket:   aws.String(sc.config.BucketName),
		Key:      aws.String(key),
		UploadId: uploadID,
		MultipartUpload: &types.CompletedMultipartUpload{
			Parts: completedParts,
		},
	})

	if err != nil {
		sc.abortMultipartUpload(uploadCtx, uploadID, key)
		return fmt.Errorf("failed to complete multipart upload: %w", err)
	}

	return nil
}

// uploadPart uploads a single part with retry logic
func (sc *S3Client) uploadPart(ctx context.Context, uploadID *string, key string, partNumber int32, data []byte) (*s3.UploadPartOutput, error) {
	for attempt := 0; attempt < sc.config.MaxRetries; attempt++ {
		resp, err := sc.s3Client.UploadPart(ctx, &s3.UploadPartInput{
			Bucket:     aws.String(sc.config.BucketName),
			Key:        aws.String(key),
			PartNumber: aws.Int32(partNumber),
			UploadId:   uploadID,
			Body:       bytes.NewReader(data),
		})

		if err == nil {
			return resp, nil
		}

		log.Printf("Upload part %d attempt %d failed: %v", partNumber, attempt+1, err)
		if attempt < sc.config.MaxRetries-1 {
			time.Sleep(time.Duration(attempt+1) * time.Second)
		}
	}

	return nil, fmt.Errorf("failed to upload part after %d attempts", sc.config.MaxRetries)
}

// abortMultipartUpload aborts a multipart upload
func (sc *S3Client) abortMultipartUpload(ctx context.Context, uploadID *string, key string) {
	_, err := sc.s3Client.AbortMultipartUpload(ctx, &s3.AbortMultipartUploadInput{
		Bucket:   aws.String(sc.config.BucketName),
		Key:      aws.String(key),
		UploadId: uploadID,
	})
	if err != nil {
		log.Printf("Failed to abort multipart upload: %v", err)
	}
}
