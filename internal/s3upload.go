package internal

import (
	"encoding/json"
	"fmt"
	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/credentials"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/s3/s3manager"
	"github.com/google/logger"
	"github.com/sudachen/coin-exchange/exchange"
	"io"
	"io/ioutil"
	"os"
	"path"
	"strings"
	"sync"
	"time"
)

var s3group = sync.WaitGroup{}

type S3Tags struct {
	StartedAt int64
	EndedAt   int64
	Count     int32
	Exchanges map[string]bool
	Pairs     map[string]bool
	Channel   string
}

func Upload(name string, cfg *Config) {
	s3group.Add(1)
	go s3worker(name, cfg)
}

func S3channelName(channel exchange.Channel) string {
	switch channel {
	case exchange.Candlestick:
		return "candlestick"
	case exchange.Trade:
		return "trade"
	case exchange.Depth:
		return "depth"
	default:
		return fmt.Sprintf("channel-%d", channel)
	}
}

func makeKey(name string, s3t *S3Tags, cfg *Config) string {
	ext := path.Ext(name)
	dt := time.Unix(s3t.StartedAt, 0).UTC()
	return fmt.Sprintf("%s%04d%02d/%s-%04d%02d%02dT%02d%02d%02d%s",
		cfg.S3.Prefix,
		dt.Year(),
		dt.Month(),
		//
		s3t.Channel,
		dt.Year(),
		dt.Month(),
		dt.Day(),
		dt.Hour(),
		dt.Minute(),
		dt.Second(),
		ext)
}

func joinKeys(m map[string]bool) string {
	keys := make([]string, 0, len(m))
	for k, b := range m {
		if b {
			keys = append(keys, k)
		}
	}
	return strings.Join(keys, " ")
}

func S3tName(name string) string {
	return strings.TrimSuffix(name, path.Ext(name)) + ".json"
}

func S3tWrite(name string, s3t *S3Tags) error {
	jsName := S3tName(name)
	jcont, _ := json.MarshalIndent(s3t, "", " ")
	return ioutil.WriteFile(jsName, jcont, 0644)
}

func s3open(name string) (io.Reader, *S3Tags, error) {
	var err error
	var bs []byte
	jsName := S3tName(name)
	if bs, err = ioutil.ReadFile(jsName); err != nil {
		return nil, nil, fmt.Errorf("can't read metadata of %v: %v", name, err.Error())
	}
	s3t := &S3Tags{}
	if err = json.Unmarshal(bs, &s3t); err != nil {
		return nil, nil, fmt.Errorf("broken metadata of %v: %v", name, err.Error())
	}
	if f, err := os.Open(name); err != nil {
		return nil, nil, fmt.Errorf("can't open file %v: %v", name, err.Error())
	} else {
		return f, s3t, nil
	}
}

func s3worker(name string, cfg *Config) {
	if cfg.S3.Endpoint == "" {
		logger.Errorf("S3 endpoint is not specified, upload skipped")
	} else {
		if f, s3t, err := s3open(name); err != nil {
			logger.Errorf("can't open file %v: %v", name, err.Error())
		} else {
			logger.Infof("s3 upload started: %v", name)
			endpoint := cfg.S3.Endpoint
			region := cfg.S3.Region
			sess := session.Must(session.NewSession(&aws.Config{
				Endpoint:    &endpoint,
				Region:      &region,
				Credentials: credentials.NewStaticCredentials(cfg.S3.Key, cfg.S3.Secret, ""),
			}))
			uploader := s3manager.NewUploader(sess)
			startedAt := time.Unix(s3t.StartedAt, 0).UTC().String()
			endedAt := time.Unix(s3t.EndedAt, 0).UTC().String()
			count := fmt.Sprint(s3t.Count)
			exchanges := joinKeys(s3t.Exchanges)
			pairs := joinKeys(s3t.Pairs)
			key := makeKey(name, s3t, cfg)

			_, err := uploader.Upload(&s3manager.UploadInput{
				Bucket: aws.String(cfg.S3.Bucket),
				Key:    aws.String(key),
				Body:   f,
				Metadata: map[string]*string{
					"started-at": &startedAt,
					"ended-at":   &endedAt,
					"count":      &count,
					"exchanges":  &exchanges,
					"pairs":      &pairs,
					"channel":    &s3t.Channel,
				},
			})

			if err != nil {
				logger.Errorf("s3 upload failed: %v", err.Error())
			} else {
				logger.Infof("s3 upload finished: %v", name)
				_ = os.Remove(name)
				_ = os.Remove(S3tName(name))
			}
		}
	}
	s3group.Done()
}

func WaitForUploads() {
	logger.Info("waiting for uploads")
	s3group.Wait()
	logger.Info("all uploads finished")
}
