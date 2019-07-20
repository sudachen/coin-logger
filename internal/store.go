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

const depthLength = 3

var cacheDirname string

type channelDef struct {
	df interface{}
	cx chan interface{}
}

type parq struct {
	tempName  string
	fileName  string
	startedAt time.Time
	source    source.ParquetFile
	writer    *writer.ParquetWriter
}

const channelLength = 31
const indexChannelLength = channelLength * 3 / 2

type Metadata interface {
	GetOrigin() exchange.Exchange
	GetPair() exchange.CoinPair
	GetTimestamp() int64
}

type record struct {
	exchange.Exchange
	exchange.CoinPair
}

type indexRecord struct {
	record
	Channel   int32 `parquet:"name=channel, type=INT32"`
	Index     int32 `parquet:"name=index, type=INT32"`
	Timestamp int64 `parquet:"name=timestamp, type=TIMESTAMP_MICROS"`
}

func (r *indexRecord) GetOrigin() exchange.Exchange {
	return r.Exchange
}

func (r *indexRecord) GetPair() exchange.CoinPair {
	return r.CoinPair
}

func (r *indexRecord) GetTimestamp() int64 {
	return r.Timestamp
}

type tradeRecord struct {
	record
	Origin    string  `parquet:"name=origin, type=UTF8, encoding=PLAIN_DICTIONARY"`
	Coin1     string  `parquet:"name=coin1, type=UTF8, encoding=PLAIN_DICTIONARY"`
	Coin2     string  `parquet:"name=coin2, type=UTF8, encoding=PLAIN_DICTIONARY"`
	Price     float32 `parquet:"name=price, type=FLOAT"`
	Qty       float32 `parquet:"name=qty, type=FLOAT"`
	Sell      bool    `parquet:"name=sell, type=BOOLEAN"`
	Timestamp int64   `parquet:"name=timestamp, type=TIMESTAMP_MICROS"`
}

func (r *tradeRecord) GetTimestamp() int64 {
	return r.Timestamp
}

func (r *tradeRecord) GetOrigin() exchange.Exchange {
	return r.Exchange
}

func (r *tradeRecord) GetPair() exchange.CoinPair {
	return r.CoinPair
}

type candleRecord struct {
	record
	Origin    string  `parquet:"name=origin, type=UTF8, encoding=PLAIN_DICTIONARY"`
	Coin1     string  `parquet:"name=coin1, type=UTF8, encoding=PLAIN_DICTIONARY"`
	Coin2     string  `parquet:"name=coin2, type=UTF8, encoding=PLAIN_DICTIONARY"`
	Interval  int32   `parquet:"name=interval, type=INT32"`
	TradeNum  int32   `parquet:"name=tradenum, type=INT32"`
	Open      float32 `parquet:"name=open, type=FLOAT"`
	Close     float32 `parquet:"name=close, type=FLOAT"`
	High      float32 `parquet:"name=high, type=FLOAT"`
	Low       float32 `parquet:"name=low, type=FLOAT"`
	Volume    float32 `parquet:"name=volume, type=FLOAT"`
	Timestamp int64   `parquet:"name=timestamp, type=TIMESTAMP_MICROS"`
}

func (r *candleRecord) GetTimestamp() int64 {
	return r.Timestamp
}

func (r *candleRecord) GetOrigin() exchange.Exchange {
	return r.Exchange
}

func (r *candleRecord) GetPair() exchange.CoinPair {
	return r.CoinPair
}

type depthRecord struct {
	record
	Origin    string `parquet:"name=origin, type=UTF8, encoding=PLAIN_DICTIONARY"`
	Coin1     string `parquet:"name=coin1, type=UTF8, encoding=PLAIN_DICTIONARY"`
	Coin2     string `parquet:"name=coin2, type=UTF8, encoding=PLAIN_DICTIONARY"`
	Timestamp int64  `parquet:"name=timestamp, type=TIMESTAMP_MICROS"`
	//BidsAvg    float32   `parquet:"name=bids_avg, type=FLOAT"`
	//BidsMedian float32   `parquet:"name=bids_median, type=FLOAT"`
	//BidsVolume float32   `parquet:"name=bids_volume, type=FLOAT"`
	//BidsSum    float32   `parquet:"name=bids_sum, type=FLOAT"`
	//AsksAvg    float32   `parquet:"name=asks_avg, type=FLOAT"`
	//AsksMedian float32   `parquet:"name=asks_median, type=FLOAT"`
	//AsksVolume float32   `parquet:"name=asks_volume, type=FLOAT"`
	//AsksSum    float32   `parquet:"name=asks_sum, type=FLOAT"`
	BidsPrice []float32 `parquet:"name=bids_price, type=LIST, valuetype=FLOAT, repetitiontype=REQUIRED"`
	BidsQty   []float32 `parquet:"name=bids_qty, type=LIST, valuetype=FLOAT, repetitiontype=REQUIRED"`
	AsksPrice []float32 `parquet:"name=asks_price, type=LIST, valuetype=FLOAT, repetitiontype=REQUIRED"`
	AsksQty   []float32 `parquet:"name=asks_qty, type=LIST, valuetype=FLOAT, repetitiontype=REQUIRED"`
}

func (r *depthRecord) GetTimestamp() int64 {
	return r.Timestamp
}

func (r *depthRecord) GetOrigin() exchange.Exchange {
	return r.Exchange
}

func (r *depthRecord) GetPair() exchange.CoinPair {
	return r.CoinPair
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
	f := fmt.Sprintf("%04d%02d%02dT%02d%02d%02d-%d.pqt",
		utc.Year(), utc.Month(), utc.Day(),
		utc.Hour(), utc.Minute(), utc.Second(),
		channel)
	if cacheDirname != "" {
		f = path.Join(cacheDirname, f)
	}
	return f
}

func tempNameFromChannel(channel exchange.Channel, t time.Time) string {
	return fileNameFromChannel(channel, t) + "~"
}

func createOneParquet(channel exchange.Channel, df interface{}, startedAt time.Time, cfg *Config) (*parq, error) {
	tempName := tempNameFromChannel(channel, startedAt)
	fileName := fileNameFromChannel(channel, startedAt)
	if fw, err := local.NewLocalFileWriter(tempName); err != nil {
		logger.Errorf("Can't create local file: %v", err)
		return nil, err
	} else {
		if pw, err := writer.NewParquetWriter(fw, df, 4); err != nil {
			logger.Errorf("Can't create parquet writer: %v", err)
			return nil, err
		} else {
			pw.RowGroupSize = 16 * 1024 * 1024 //10M
			return &parq{tempName, fileName, startedAt, fw, pw}, nil
		}
	}
}

type Channels struct {
	channels map[exchange.Channel]*channelDef
	sync.WaitGroup
}

func (self *Channels) worker(channel exchange.Channel, df interface{}, cr chan interface{}, ci chan interface{}, startedAt time.Time, cfg *Config) {

	parq, err := createOneParquet(channel, df, startedAt, cfg)
	indexCloseCount := 0

	if err != nil {
		logger.Fatal(err.Error())
	} else {

		//logger.Infof("writer started: %s\n", parq.fileName)

		s3t := &S3Tags{
			Exchanges: make(map[int32]int32),
			Pairs:     make(map[int32]int32),
			ChannelNo: int32(channel),
			StartedAt: startedAt.Unix(),
		}

		var timemark map[int32]int64

		for {

			r, ok := <-cr
			if !ok {
				break
			}

			if channel == exchange.NoChannel {
				if _, ok := r.(*struct{}); ok {
					indexCloseCount += 1
					if indexCloseCount >= len(self.channels) {
						break
					}
					continue
				}
			}

			m := r.(Metadata)
			origin := m.GetOrigin()
			pair := m.GetPair()
			index := s3t.Count
			timestamp := m.GetTimestamp() // usec

			if channel != exchange.NoChannel {
				s3t.Exchanges[int32(origin)] += 1
				s3t.Pairs[pair.AsInt()] += 1
				s3t.Count += 1
				if err := parq.writer.Write(r); err != nil {
					logger.Fatal(err.Error())
				}
				ci <- &indexRecord{
					record{origin, pair},
					int32(channel),
					index,
					timestamp,
				}
			} else {
				s3t.Exchanges[int32(origin)] = 1
				s3t.Pairs[pair.AsInt()] = 1
				ir := r.(*indexRecord)
				minutes := (timestamp/1000000-s3t.StartedAt)/60 + 1
				if timemark == nil {
					timemark = make(map[int32]int64)
				}
				if minutes > timemark[ir.Channel] {
					s3t.Count += 1
					timemark[ir.Channel] = minutes
					if err := parq.writer.Write(r); err != nil {
						logger.Fatal(err.Error())
					}
				}
			}
		}

		if channel != exchange.NoChannel {
			ci <- &struct{}{}
		}

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

		logger.Infof("writer finished: %s\n", parq.fileName)
	}

	self.WaitGroup.Done()
}

func (self *Channels) Close(wait bool) {

	for _, v := range self.channels {
		if v.cx != nil {
			close(v.cx)
			v.cx = nil
		}
	}

	if wait {
		self.WaitGroup.Wait()
	}
}

func (self *Channels) Open(cfg *Config) {
	t := time.Now()
	ci := make(chan interface{}, indexChannelLength)
	for c, v := range self.channels {
		cr := make(chan interface{}, channelLength)
		v.cx = cr
		self.WaitGroup.Add(1)
		go self.worker(c, v.df, cr, ci, t, cfg)
	}
	self.WaitGroup.Add(1)
	go self.worker(exchange.NoChannel, &indexRecord{}, ci, nil, t, cfg)
}

type Writer struct {
	*Config
	cClose chan struct{}
	done   sync.WaitGroup
}

func (self *Writer) worker() {

	var ch = Channels{
		map[exchange.Channel]*channelDef{
			exchange.Trade: &channelDef{
				&tradeRecord{},
				nil,
			},
			exchange.Candlestick: &channelDef{
				&candleRecord{},
				nil,
			},
			exchange.Depth: &channelDef{
				&depthRecord{},
				nil,
			},
		},
		sync.WaitGroup{},
	}

	var ticker *time.Ticker
	if times := self.Config.S3.Times; times != 0 {
		ticker = time.NewTicker(time.Duration(times) * time.Minute)
	} else {
		ticker = time.NewTicker(8 * time.Hour)
	}
	defer ticker.Stop()

	self.cClose = make(chan struct{})
	ch.Open(self.Config)

	for {
		select {
		case <-ticker.C:
			ch.Close(false)
			ch.Open(self.Config)
		case <-self.cClose:
			ch.Close(true)
			self.done.Done()
			return
		case e := <-exchange.Collector.Messages:
			//logger.Infof("msg: %#v",e)
			switch msg := e.(type) {
			case *message.Trade:
				ch.channels[exchange.Trade].cx <- &tradeRecord{
					record:    record{msg.Origin, msg.Pair},
					Origin:    msg.Origin.String(),
					Coin1:     msg.Pair[0].String(),
					Coin2:     msg.Pair[1].String(),
					Price:     msg.Price,
					Qty:       msg.Qty,
					Sell:      msg.Sell,
					Timestamp: msg.Timestamp.UTC().UnixNano() / 1000,
				}
			case *message.Candlestick:
				ch.channels[exchange.Candlestick].cx <- &candleRecord{
					record:    record{msg.Origin, msg.Pair},
					Origin:    msg.Origin.String(),
					Coin1:     msg.Pair[0].String(),
					Coin2:     msg.Pair[1].String(),
					Interval:  msg.Interval,
					TradeNum:  msg.TradeNum,
					Open:      msg.Open,
					Close:     msg.Close,
					High:      msg.High,
					Low:       msg.Low,
					Volume:    msg.Volume,
					Timestamp: msg.Timestamp.UTC().UnixNano() / 1000,
				}
			case *message.Depth:
				r := &depthRecord{
					record:    record{msg.Origin, msg.Pair},
					Origin:    msg.Origin.String(),
					Coin1:     msg.Pair[0].String(),
					Coin2:     msg.Pair[1].String(),
					Timestamp: msg.Timestamp.UTC().UnixNano() / 1000,
					BidsPrice: make([]float32, depthLength),
					BidsQty:   make([]float32, depthLength),
					AsksPrice: make([]float32, depthLength),
					AsksQty:   make([]float32, depthLength),
					//BidsAvg:    msg.AggBids.Avg,
					//BidsMedian: msg.AggBids.Median,
					//BidsVolume: msg.AggBids.Volume,
					//BidsSum:    msg.AggBids.Qty,
					//AsksAvg:    msg.AggAsks.Avg,
					//AsksMedian: msg.AggAsks.Median,
					//AsksVolume: msg.AggAsks.Volume,
					//AsksSum:    msg.AggAsks.Qty,
				}
				for i, v := range msg.Bids {
					if i < depthLength {
						r.BidsPrice[i] = v.Price
						r.BidsQty[i] = v.Qty
					} else {
						break
					}
				}
				for i, v := range msg.Asks {
					if i < depthLength {
						r.AsksPrice[i] = v.Price
						r.AsksQty[i] = v.Qty
					} else {
						break
					}
				}
				ch.channels[exchange.Depth].cx <- r
			}
		}
	}
}

func (self *Writer) Start() error {

	if dirname, err := cacheDir(self.Config); err != nil {
		return err
	} else {
		cacheDirname = *dirname
	}

	self.done.Add(1)
	go self.worker()
	return nil
}

func (self *Writer) Stop() {

	close(self.cClose)
	//logger.Info("waiting for writers\n")

	self.done.Wait()
	logger.Info("all writers finished\n")

	WaitForUploads()
}
