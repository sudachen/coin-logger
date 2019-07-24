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
	"github.com/sudachen/coin-exchange/exchange/channel"
	"io"
	"io/ioutil"
	"os"
	"path"
	"strings"
	"sync"
	"time"
)

const VersionString = "13"

var s3group = sync.WaitGroup{}

type S3Tags struct {
	StartedAt int64
	EndedAt   int64
	Count     int32
	Exchanges map[int32]int32
	Pairs     map[int32]int32
	ChannelNo int32
}

type S3Meta struct {
	StartedAt string
	EndedAt   string
	Count     int32
	Exchanges map[string]int32
	Pairs     map[string]int32
	Channel   string
}

func Upload(name string, cfg *Config) {
	s3group.Add(1)
	go s3worker(name, cfg)
}

func makeKey(name string, s3t *S3Tags, cfg *Config) string {
	ext := path.Ext(name)
	dt := time.Unix(s3t.StartedAt, 0).UTC()
	_, week := dt.ISOWeek()
	return fmt.Sprintf("%s%04d%02d.%02d.%s/%04d%02d%02dT%02d%02d%02d-%d%s",
		cfg.S3.Prefix,
		dt.Year(),
		dt.Month(),
		week,
		VersionString,
		//
		dt.Year(),
		dt.Month(),
		dt.Day(),
		dt.Hour(),
		dt.Minute(),
		dt.Second(),
		s3t.ChannelNo,
		ext)
}

func mapKeys(m map[int32]int32, cv func(int32) string) map[string]int32 {
	r := make(map[string]int32)
	for k, n := range m {
		if n != 0 {
			r[cv(k)] = n
		}
	}
	return r
}

func joinKeys(m map[int32]int32, cv func(int32) string) string {
	keys := make([]string, 0, len(m))
	for k, n := range m {
		if n != 0 {
			keys = append(keys, cv(k))
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

func S3Channel(c int32) string {
	ch := channel.Channel(c)
	switch ch {
	case ChIndex:
		return "Index"
	case ChSnapshot:
		return "Snapshot"
	default:
		return ch.String()
	}
}

func s3worker(name string, cfg *Config) {
	if cfg.S3.Endpoint == "" {
		logger.Errorf("S3 endpoint is not specified, upload skipped")
	} else {
		if f, s3t, err := s3open(name); err != nil {
			logger.Errorf("can't open file %v: %v", name, err.Error())
		} else {
			//logger.Infof("s3 upload started: %v", name)
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
			exchanges := joinKeys(s3t.Exchanges, func(e int32) string { return exchange.Exchange(e).String() })
			pairs := joinKeys(s3t.Pairs, func(p int32) string { return (&exchange.CoinPair{}).FromInt(p).String() })
			key := makeKey(name, s3t, cfg)
			version := VersionString
			ch := S3Channel(s3t.ChannelNo)

			bs, _ := json.Marshal(&S3Meta{
				StartedAt: startedAt,
				EndedAt:   endedAt,
				Exchanges: mapKeys(s3t.Exchanges, func(e int32) string { return exchange.Exchange(e).String() }),
				Pairs:     mapKeys(s3t.Pairs, func(p int32) string { return (&exchange.CoinPair{}).FromInt(p).String() }),
				Channel:   ch,
				Count:     s3t.Count,
			})
			detail := string(bs)

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
					"channel":    &ch,
					"version":    &version,
					"z-detail":   &detail,
				},
			})

			if err != nil {
				logger.Errorf("s3 upload failed: %v", err.Error())
			} else {
				logger.Infof("s3 uploaded: %v", name)
				_ = os.Remove(name)
				_ = os.Remove(S3tName(name))
			}
		}
	}
	s3group.Done()
}

func WaitForUploads() {
	//logger.Info("waiting for uploads")
	s3group.Wait()
	logger.Info("all uploads finished")
}
