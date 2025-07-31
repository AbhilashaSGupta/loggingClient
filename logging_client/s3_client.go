package logging_client

import (
	"bytes"
	"context"
	"fmt"
	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/aws/aws-sdk-go-v2/service/s3/types"
	"log"
	"time"
)

// uploadToS3 uploads data to S3 using multipart upload for large data
func (lc *LoggingClient) uploadToS3(key string, data []byte) error {
	dataSize := int64(len(data))

	// Use simple put for small files
	if dataSize < lc.Config.ChunkSize {
		return lc.simplePutObject(key, data)
	}

	// Use multipart upload for larger files
	return lc.multipartUpload(key, data)
}

// simplePutObject uploads data using a simple PUT operation
func (lc *LoggingClient) simplePutObject(key string, data []byte) error {
	ctx, cancel := context.WithTimeout(lc.Ctx, 30*time.Second)
	defer cancel()

	for attempt := 0; attempt < lc.Config.MaxRetries; attempt++ {
		_, err := lc.S3Client.PutObject(ctx, &s3.PutObjectInput{
			Bucket:      aws.String(lc.Config.BucketName),
			Key:         aws.String(key),
			Body:        bytes.NewReader(data),
			ContentType: aws.String("application/jsonl"),
		})

		if err == nil {
			return nil
		}

		log.Printf("Put object attempt %d failed: %v", attempt+1, err)
		if attempt < lc.Config.MaxRetries-1 {
			time.Sleep(time.Duration(attempt+1) * time.Second)
		}
	}

	return fmt.Errorf("failed to put object after %d attempts", lc.Config.MaxRetries)
}

// multipartUpload uploads data using S3 multipart upload
func (lc *LoggingClient) multipartUpload(key string, data []byte) error {
	ctx, cancel := context.WithTimeout(lc.Ctx, 5*time.Minute)
	defer cancel()

	// Create multipart upload
	createResp, err := lc.S3Client.CreateMultipartUpload(ctx, &s3.CreateMultipartUploadInput{
		Bucket:      aws.String(lc.Config.BucketName),
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

	for offset := int64(0); offset < dataLen; offset += lc.Config.ChunkSize {
		end := offset + lc.Config.ChunkSize
		if end > dataLen {
			end = dataLen
		}

		partData := data[offset:end]

		uploadResp, err := lc.uploadPart(ctx, uploadID, key, partNumber, partData)
		if err != nil {
			// Abort multipart upload on error
			lc.abortMultipartUpload(ctx, uploadID, key)
			return fmt.Errorf("failed to upload part %d: %w", partNumber, err)
		}

		completedParts = append(completedParts, types.CompletedPart{
			ETag:       uploadResp.ETag,
			PartNumber: aws.Int32(partNumber),
		})

		partNumber++
	}

	// Complete multipart upload
	_, err = lc.S3Client.CompleteMultipartUpload(ctx, &s3.CompleteMultipartUploadInput{
		Bucket:   aws.String(lc.Config.BucketName),
		Key:      aws.String(key),
		UploadId: uploadID,
		MultipartUpload: &types.CompletedMultipartUpload{
			Parts: completedParts,
		},
	})

	if err != nil {
		lc.abortMultipartUpload(ctx, uploadID, key)
		return fmt.Errorf("failed to complete multipart upload: %w", err)
	}

	return nil
}

// uploadPart uploads a single part with retry logic
func (lc *LoggingClient) uploadPart(ctx context.Context, uploadID *string, key string, partNumber int32, data []byte) (*s3.UploadPartOutput, error) {
	for attempt := 0; attempt < lc.Config.MaxRetries; attempt++ {
		resp, err := lc.S3Client.UploadPart(ctx, &s3.UploadPartInput{
			Bucket:     aws.String(lc.Config.BucketName),
			Key:        aws.String(key),
			PartNumber: aws.Int32(partNumber),
			UploadId:   uploadID,
			Body:       bytes.NewReader(data),
		})

		if err == nil {
			return resp, nil
		}

		log.Printf("Upload part %d attempt %d failed: %v", partNumber, attempt+1, err)
		if attempt < lc.Config.MaxRetries-1 {
			time.Sleep(time.Duration(attempt+1) * time.Second)
		}
	}

	return nil, fmt.Errorf("failed to upload part after %d attempts", lc.Config.MaxRetries)
}

// abortMultipartUpload aborts a multipart upload
func (lc *LoggingClient) abortMultipartUpload(ctx context.Context, uploadID *string, key string) {
	_, err := lc.S3Client.AbortMultipartUpload(ctx, &s3.AbortMultipartUploadInput{
		Bucket:   aws.String(lc.Config.BucketName),
		Key:      aws.String(key),
		UploadId: uploadID,
	})
	if err != nil {
		log.Printf("Failed to abort multipart upload: %v", err)
	}
}
