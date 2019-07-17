package internal

import (
	"fmt"
	"github.com/google/logger"
	"github.com/sudachen/coin-exchange/exchange"
	"github.com/sudachen/coin-exchange/exchange/message"
	"github.com/xitongsys/parquet-go-source/local"
	"github.com/xitongsys/parquet-go/source"
	"github.com/xitongsys/parquet-go/writer"
	"os"
	"path"
	"sync"
	"time"
)

var cacheDirname string
var waitGroup sync.WaitGroup

type channelDef struct {
	df    interface{}
	cx    chan interface{}
	close chan struct{}
}

type parq struct {
	tempName  string
	fileName  string
	startedAt time.Time
	source    source.ParquetFile
	writer    *writer.ParquetWriter
}

var channels = map[exchange.Channel]*channelDef{
	exchange.Trade: &channelDef{
		&tradeRecord{},
		make(chan interface{}, 100),
		nil,
	},
	exchange.Candlestick: &channelDef{
		&candleRecord{},
		make(chan interface{}, 100),
		nil,
	},
	exchange.Depth: &channelDef{
		&depthRecord{},
		make(chan interface{}, 100),
		nil,
	},
}

type Metadata interface {
	GetOrigin() string
	GetPair() string
}

type tradeRecord struct {
	Origin         string  `parquet:"name=origin, type=UTF8, encoding=PLAIN_DICTIONARY"`
	Coin1          string  `parquet:"name=coin1, type=UTF8, encoding=PLAIN_DICTIONARY"`
	Coin2          string  `parquet:"name=coin2, type=UTF8, encoding=PLAIN_DICTIONARY"`
	TradeId        int64   `parquet:"name=trade_id, type=INT64"`
	Price          float32 `parquet:"name=price, type=FLOAT"`
	Qty            float32 `parquet:"name=qty, type=FLOAT"`
	BuyerOrderId   int64   `parquet:"name=bayer_order_id, type=INT64"`
	SellerOrderId  int64   `parquet:"name=seller_order_id, type=INT64"`
	TradeOrderTime int64   `parquet:"name=trader_time, type=TIMESTAMP_MICROS"`
}

func (c *tradeRecord) GetOrigin() string {
	return c.Origin
}

func (c *tradeRecord) GetPair() string {
	return c.Coin1 + "/" + c.Coin2
}

type candleRecord struct {
	Origin       string  `parquet:"name=origin, type=UTF8, encoding=PLAIN_DICTIONARY"`
	Coin1        string  `parquet:"name=coin1, type=UTF8, encoding=PLAIN_DICTIONARY"`
	Coin2        string  `parquet:"name=coin2, type=UTF8, encoding=PLAIN_DICTIONARY"`
	StartTime    int64   `parquet:"name=start_time, type=TIMESTAMP_MICROS"`
	EndTime      int64   `parquet:"name=end_time, type=TIMESTAMP_MICROS"`
	Interval     int32   `parquet:"name=interval, type=INT32"`
	TradeNum     int32   `parquet:"name=tradenum, type=INT32"`
	FirstTradeId int64   `parquet:"name=first_trade_id, type=INT64"`
	LastTradeId  int64   `parquet:"name=last_trade_id, type=INT64"`
	Open         float32 `parquet:"name=open, type=FLOAT"`
	Close        float32 `parquet:"name=close, type=FLOAT"`
	High         float32 `parquet:"name=high, type=FLOAT"`
	Low          float32 `parquet:"name=low, type=FLOAT"`
	Volume       float32 `parquet:"name=volume, type=FLOAT"`
}

func (c *candleRecord) GetOrigin() string {
	return c.Origin
}

func (c *candleRecord) GetPair() string {
	return c.Coin1 + "/" + c.Coin2
}

type depthRecord struct {
	Origin        string    `parquet:"name=origin, type=UTF8, encoding=PLAIN_DICTIONARY"`
	Coin1         string    `parquet:"name=coin1, type=UTF8, encoding=PLAIN_DICTIONARY"`
	Coin2         string    `parquet:"name=coin2, type=UTF8, encoding=PLAIN_DICTIONARY"`
	FirstUpdateId int64     `parquet:"name=first_update_id, type=INT64"`
	LastUpdateId  int64     `parquet:"name=last_update_id, type=INT64"`
	BidsPrice     []float32 `parquet:"name=bids_price, type=LIST, valuetype=FLOAT, repetitiontype=REQUIRED"`
	BidsQty       []float32 `parquet:"name=bids_qty, type=LIST, valuetype=FLOAT, repetitiontype=REQUIRED"`
	AsksPrice     []float32 `parquet:"name=asks_price, type=LIST, valuetype=FLOAT, repetitiontype=REQUIRED"`
	AsksQty       []float32 `parquet:"name=asks_qty, type=LIST, valuetype=FLOAT, repetitiontype=REQUIRED"`
}

func (c *depthRecord) GetOrigin() string {
	return c.Origin
}

func (c *depthRecord) GetPair() string {
	return c.Coin1 + "/" + c.Coin2
}

func cacheDir(cfg *Config) (*string, error) {
	dirname := cfg.S3.Cache
	if dir1, _ := path.Split(dirname); dir1 == "" {
		s3cache, ok := os.LookupEnv("S3_CACHE")
		if ok {
			dirname = path.Join(s3cache, dirname)
		}
	}
	if st, err := os.Stat(dirname); err != nil {
		if err = os.MkdirAll(dirname, 0700); err != nil {
			return nil, fmt.Errorf("can't create cache directory %v", dirname)
		}
	} else if !st.IsDir() {
		return nil, fmt.Errorf("is not directory: %v", dirname)
	}
	return &dirname, nil
}

func fileNameFromChannel(channel exchange.Channel, t time.Time) string {
	utc := t.UTC()
	f := fmt.Sprintf("%s-%04d%02d%02dT%02d%02d%02d.parquet",
		S3channelName(channel),
		utc.Year(), utc.Month(), utc.Day(),
		utc.Hour(), utc.Minute(), utc.Second())
	if cacheDirname != "" {
		f = path.Join(cacheDirname, f)
	}
	return f
}

func tempNameFromChannel(channel exchange.Channel, t time.Time) string {
	return fileNameFromChannel(channel,t) + "~"
}

func createOneParquet(channel exchange.Channel, startedAt time.Time, cfg *Config) (*parq, error) {
	tempName := tempNameFromChannel(channel, startedAt)
	fileName := fileNameFromChannel(channel, startedAt)
	if fw, err := local.NewLocalFileWriter(tempName); err != nil {
		logger.Errorf("Can't create local file: %v", err)
		return nil, err
	} else {
		if pw, err := writer.NewParquetWriter(fw, channels[channel].df, 4); err != nil {
			logger.Errorf("Can't create parquet writer: %v", err)
			return nil, err
		} else {
			return &parq{tempName, fileName, startedAt, fw, pw}, nil
		}
	}
}

func worker1(channel exchange.Channel, startedAt time.Time, cfg *Config) {
	done := false
	c := make(chan struct{}, 1)
	channels[channel].close = c
	parq, err := createOneParquet(channel, startedAt, cfg)
	if err != nil {
		logger.Fatal(err.Error())
	} else {
		s3t := &S3Tags{
			Exchanges: make(map[string]bool),
			Pairs:     make(map[string]bool),
			Channel:   channel.String(),
			StartedAt: startedAt.Unix(),
		}
		for !done {
			select {
			case <-c:
				close(c)
				s3t.EndedAt = time.Now().Unix()
				e1 := parq.writer.WriteStop()
				if err := parq.source.Close(); err == nil && e1 == nil {
					_ = os.Rename(parq.tempName, parq.fileName)
					if err := S3tWrite(parq.fileName, s3t); err != nil {
						logger.Errorf("failed to write metadata: %v", err.Error())
					} else {
						Upload(parq.fileName, cfg)
					}
				}
				done = true
			case r := <-channels[channel].cx:
				m := r.(Metadata)
				s3t.Count += 1
				s3t.Exchanges[m.GetOrigin()] = true
				s3t.Pairs[m.GetPair()] = true
				if err := parq.writer.Write(r); err != nil {
					logger.Fatal(err.Error())
				}
			}
		}
	}
	if parq != nil {
		logger.Infof("parquet done: %s\n", parq.fileName)
	}
	waitGroup.Done()
}

func closeStore() {
	for _, v := range channels {
		if v.close != nil {
			v.close <- struct{}{}
		}
	}
	waitGroup.Wait()
}

func openStore(cfg *Config) {
	closeStore()
	t := time.Now()
	for c, _ := range channels {
		waitGroup.Add(1)
		go worker1(c, t, cfg)
	}
}

func reopenStore(cfg *Config) {
	for _, v := range channels {
		if v.close != nil {
			v.close <- struct{}{}
		}
	}
	t := time.Now()
	for c, _ := range channels {
		waitGroup.Add(1)
		go worker1(c, t, cfg)
	}
}

var writerClose chan struct{}
var writerDone sync.WaitGroup

func Writer(cfg *Config) {
	var ticker *time.Ticker
	if times := cfg.S3.Times; times != 0 {
		ticker = time.NewTicker(time.Duration(times) * time.Minute)
	} else {
		ticker = time.NewTicker(8 * time.Hour)
	}
	defer ticker.Stop()

	writerClose = make(chan struct{})
	openStore(cfg)
	for {
		select {
		case <-ticker.C:
			reopenStore(cfg)
		case <-writerClose:
			closeStore()
			close(writerClose)
			writerDone.Done()
			return
		case e := <-exchange.Collector.Messages:
			//logger.Infof("msg: %#v",e)
			switch msg := e.(type) {
			case *message.Trade:
				channels[exchange.Trade].cx <- &tradeRecord{
					Origin:         msg.Origin.String(),
					Coin1:          msg.Pair[0].String(),
					Coin2:          msg.Pair[1].String(),
					TradeId:        msg.TradeId,
					Price:          msg.Price,
					Qty:            msg.Qty,
					BuyerOrderId:   msg.BuyerOrderId,
					SellerOrderId:  msg.SellerOrderId,
					TradeOrderTime: msg.TradeOrderTime.UnixNano() / 1000,
				}
			case *message.Candlestick:
				channels[exchange.Candlestick].cx <- &candleRecord{
					Origin:       msg.Origin.String(),
					Coin1:        msg.Pair[0].String(),
					Coin2:        msg.Pair[1].String(),
					StartTime:    msg.StartTime.UnixNano() / 1000,
					EndTime:      msg.EndTime.UnixNano() / 1000,
					Interval:     msg.Interval,
					TradeNum:     msg.TradeNum,
					FirstTradeId: msg.FirstTradeId,
					LastTradeId:  msg.LastTradeId,
					Open:         msg.Open,
					Close:        msg.Close,
					High:         msg.High,
					Low:          msg.Low,
					Volume:       msg.Volume,
				}
			case *message.Depth:
				r := &depthRecord{
					Origin:        msg.Origin.String(),
					Coin1:         msg.Pair[0].String(),
					Coin2:         msg.Pair[1].String(),
					FirstUpdateId: msg.FirstUpdateId,
					LastUpdateId:  msg.LastUpdateId,
					BidsPrice:     make([]float32, len(msg.Bids)),
					BidsQty:       make([]float32, len(msg.Bids)),
					AsksPrice:     make([]float32, len(msg.Asks)),
					AsksQty:       make([]float32, len(msg.Asks)),
				}
				for i, v := range msg.Bids {
					r.BidsPrice[i] = v.Price
					r.BidsQty[i] = v.Qty
				}
				for i, v := range msg.Asks {
					r.AsksPrice[i] = v.Price
					r.AsksQty[i] = v.Qty
				}
				channels[exchange.Depth].cx <- r
			}
		}
	}
}

func StartWriter(cfg *Config) error {
	if dirname, err := cacheDir(cfg); err != nil {
		return err
	} else {
		cacheDirname = *dirname
	}

	writerDone.Add(1)
	go Writer(cfg)
	return nil
}

func StopWriter() {
	writerClose <- struct{}{}
	logger.Info("waiting for writers\n")
	writerDone.Wait()
	logger.Info("all writers finished\n")
}
