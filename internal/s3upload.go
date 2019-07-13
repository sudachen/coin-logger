package internal

import (
	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/credentials"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/s3/s3manager"
	"github.com/google/logger"
	"os"
	"path"
	"strings"
	"sync"
)

var s3group = sync.WaitGroup{}

func Upload(name string, cfg *Config) {
	if err := os.Rename(name, name+"~"); err != nil {
		logger.Errorf(" can't rename for upload: %v", name)
		return
	}
	s3group.Add(1)
	go s3worker(name+"~", cfg)
}

func makeKey(name string, cfg *Config) string {
	n := path.Base(name[:len(name)-1])
	dt := strings.SplitAfter(
			strings.FieldsFunc(n, func(c rune) bool { return c == '-'})[1],
			"T")[0]
	return cfg.S3.Prefix + dt[:len(dt)-1] + "/" + n
}

func s3worker(name string, cfg *Config) {
	if f, err := os.Open(name); err != nil {
		logger.Errorf("can't open file %v: %v", name, err.Error())
	} else {
		logger.Infof("s3 upload started: %v",name)
		// upload
		endpoint := cfg.S3.Endpoint
		region := cfg.S3.Region
		sess := session.Must(session.NewSession(&aws.Config{
			Endpoint: &endpoint,
			Region:   &region,
			Credentials: credentials.NewStaticCredentials(cfg.S3.Key, cfg.S3.Secret, ""),
		}))
		uploader := s3manager.NewUploader(sess)
		logger.Infof("%#v", cfg.S3)
		key := makeKey(name, cfg)
		_, err := uploader.Upload(&s3manager.UploadInput{
			Bucket: aws.String(cfg.S3.Bucket),
			Key:    aws.String(key),
			Body:   f,
		})
		if err != nil {
			logger.Errorf("s3 upload failed: %v", err.Error())
		} else {
			logger.Infof("s3 upload finished: %v",name)
		}
	}
	s3group.Done()
}

func WaitForUploads() {
	logger.Info("waiting for uploads")
	s3group.Wait()
	logger.Info("all uploads finished")
}
