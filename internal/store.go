package internal

import (
	"fmt"
	"github.com/google/logger"
	"github.com/sudachen/coin-exchange/exchange"
	"github.com/sudachen/coin-exchange/exchange/channel"
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

type Kline struct {
	Count     int32     `parquet:"name=count, type=INT32"`
	Interval  []int32   `parquet:"name=interval, type=LIST, valuetype=INT32, repetitiontype=REQUIRED"`
	Open      []float32 `parquet:"name=open, type=LIST, valuetype=FLOAT, repetitiontype=REQUIRED"`
	Close     []float32 `parquet:"name=close, type=LIST, valuetype=FLOAT, repetitiontype=REQUIRED"`
	High      []float32 `parquet:"name=high, type=LIST, valuetype=FLOAT, repetitiontype=REQUIRED"`
	Low       []float32 `parquet:"name=low, type=LIST, valuetype=FLOAT, repetitiontype=REQUIRED"`
	Volume    []float32 `parquet:"name=volume, type=LIST, valuetype=FLOAT, repetitiontype=REQUIRED"`
	Timestamp []int64   `parquet:"name=timestamp, type=LIST, valuetype=TIMESTAMP_MICROS, repetitiontype=REQUIRED"`
}

func (k *snapRecord) fill1(src []message.Kline) {
	count := len(src)
	//k.Kl1Interval = make ([]int32,count)
	k.Kl1Timestamp = make([]int64, count)
	k.Kl1Open = make([]float32, count)
	k.Kl1Close = make([]float32, count)
	k.Kl1High = make([]float32, count)
	k.Kl1Low = make([]float32, count)
	k.Kl1Volume = make([]float32, count)
	for i, v := range src {
		//k.Kl1Interval[i] = v.Interval
		k.Kl1Timestamp[i] = v.Timestamp.UTC().UnixNano() / 1000
		k.Kl1Open[i] = v.Open
		k.Kl1Close[i] = v.Close
		k.Kl1Low[i] = v.Low
		k.Kl1High[i] = v.High
		k.Kl1Volume[i] = v.Volume
	}
}

func (k *snapRecord) fill10(src []message.Kline) {
	count := len(src)
	//k.Kl10Interval = make ([]int32,count)
	k.Kl10Timestamp = make([]int64, count)
	k.Kl10Open = make([]float32, count)
	k.Kl10Close = make([]float32, count)
	k.Kl10High = make([]float32, count)
	k.Kl10Low = make([]float32, count)
	k.Kl10Volume = make([]float32, count)
	for i, v := range src {
		//k.Kl10Interval[i] = v.Interval
		k.Kl10Timestamp[i] = v.Timestamp.UTC().UnixNano() / 1000
		k.Kl10Open[i] = v.Open
		k.Kl10Close[i] = v.Close
		k.Kl10Low[i] = v.Low
		k.Kl10High[i] = v.High
		k.Kl10Volume[i] = v.Volume
	}
}

func (k *snapRecord) fill60(src []message.Kline) {
	count := len(src)
	//k.Kl60Interval = make ([]int32,count)
	k.Kl60Timestamp = make([]int64, count)
	k.Kl60Open = make([]float32, count)
	k.Kl60Close = make([]float32, count)
	k.Kl60High = make([]float32, count)
	k.Kl60Low = make([]float32, count)
	k.Kl60Volume = make([]float32, count)
	for i, v := range src {
		//k.Kl60Interval[i] = v.Interval
		k.Kl60Timestamp[i] = v.Timestamp.UTC().UnixNano() / 1000
		k.Kl60Open[i] = v.Open
		k.Kl60Close[i] = v.Close
		k.Kl60Low[i] = v.Low
		k.Kl60High[i] = v.High
		k.Kl60Volume[i] = v.Volume
	}
}

type snapRecord struct {
	record
	Origin    string `parquet:"name=origin, type=UTF8, encoding=PLAIN_DICTIONARY"`
	Coin1     string `parquet:"name=coin1, type=UTF8, encoding=PLAIN_DICTIONARY"`
	Coin2     string `parquet:"name=coin2, type=UTF8, encoding=PLAIN_DICTIONARY"`
	Timestamp int64  `parquet:"name=timestamp, type=TIMESTAMP_MICROS"`

	BidsPrice []float32 `parquet:"name=bids_price, type=LIST, valuetype=FLOAT, repetitiontype=REQUIRED"`
	BidsQty   []float32 `parquet:"name=bids_qty, type=LIST, valuetype=FLOAT, repetitiontype=REQUIRED"`
	AsksPrice []float32 `parquet:"name=asks_price, type=LIST, valuetype=FLOAT, repetitiontype=REQUIRED"`
	AsksQty   []float32 `parquet:"name=asks_qty, type=LIST, valuetype=FLOAT, repetitiontype=REQUIRED"`

	//Kl1Interval   []int32   `parquet:"name=kl1_interval, type=LIST, valuetype=INT32, repetitiontype=REQUIRED"`
	Kl1Open      []float32 `parquet:"name=kl1_open, type=LIST, valuetype=FLOAT, repetitiontype=REQUIRED"`
	Kl1Close     []float32 `parquet:"name=kl1_close, type=LIST, valuetype=FLOAT, repetitiontype=REQUIRED"`
	Kl1High      []float32 `parquet:"name=kl1_high, type=LIST, valuetype=FLOAT, repetitiontype=REQUIRED"`
	Kl1Low       []float32 `parquet:"name=kl1_low, type=LIST, valuetype=FLOAT, repetitiontype=REQUIRED"`
	Kl1Volume    []float32 `parquet:"name=kl1_volume, type=LIST, valuetype=FLOAT, repetitiontype=REQUIRED"`
	Kl1Timestamp []int64   `parquet:"name=kl1_timestamp, type=LIST, valuetype=TIMESTAMP_MICROS, repetitiontype=REQUIRED"`

	//Kl10Interval  []int32   `parquet:"name=kl10_interval, type=LIST, valuetype=INT32, repetitiontype=REQUIRED"`
	Kl10Open      []float32 `parquet:"name=kl10_open, type=LIST, valuetype=FLOAT, repetitiontype=REQUIRED"`
	Kl10Close     []float32 `parquet:"name=kl10_close, type=LIST, valuetype=FLOAT, repetitiontype=REQUIRED"`
	Kl10High      []float32 `parquet:"name=kl10_high, type=LIST, valuetype=FLOAT, repetitiontype=REQUIRED"`
	Kl10Low       []float32 `parquet:"name=kl10_low, type=LIST, valuetype=FLOAT, repetitiontype=REQUIRED"`
	Kl10Volume    []float32 `parquet:"name=kl10_volume, type=LIST, valuetype=FLOAT, repetitiontype=REQUIRED"`
	Kl10Timestamp []int64   `parquet:"name=kl10_timestamp, type=LIST, valuetype=TIMESTAMP_MICROS, repetitiontype=REQUIRED"`

	//Kl60Interval  []int32   `parquet:"name=kl60_interval, type=LIST, valuetype=INT32, repetitiontype=REQUIRED"`
	Kl60Open      []float32 `parquet:"name=kl60_open, type=LIST, valuetype=FLOAT, repetitiontype=REQUIRED"`
	Kl60Close     []float32 `parquet:"name=kl60_close, type=LIST, valuetype=FLOAT, repetitiontype=REQUIRED"`
	Kl60High      []float32 `parquet:"name=kl60_high, type=LIST, valuetype=FLOAT, repetitiontype=REQUIRED"`
	Kl60Low       []float32 `parquet:"name=kl60_low, type=LIST, valuetype=FLOAT, repetitiontype=REQUIRED"`
	Kl60Volume    []float32 `parquet:"name=kl60_volume, type=LIST, valuetype=FLOAT, repetitiontype=REQUIRED"`
	Kl60Timestamp []int64   `parquet:"name=kl60_timestamp, type=LIST, valuetype=TIMESTAMP_MICROS, repetitiontype=REQUIRED"`

	TdPrice     []float32 `parquet:"name=td_price, type=LIST, valuetype=FLOAT, repetitiontype=REQUIRED"`
	TdQty       []float32 `parquet:"name=td_qty, type=LIST, valuetype=FLOAT, repetitiontype=REQUIRED"`
	TdTimestamp []int64   `parquet:"name=td_timestamp, type=LIST, valuetype=TIMESTAMP_MICROS, repetitiontype=REQUIRED"`
}

func (r *snapRecord) GetTimestamp() int64 {
	return r.Timestamp
}

func (r *snapRecord) GetOrigin() exchange.Exchange {
	return r.Exchange
}

func (r *snapRecord) GetPair() exchange.CoinPair {
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

func fileNameFromChannel(ch channel.Channel, t time.Time) string {
	utc := t.UTC()
	f := fmt.Sprintf("%04d%02d%02dT%02d%02d%02d-%d.pqt",
		utc.Year(), utc.Month(), utc.Day(),
		utc.Hour(), utc.Minute(), utc.Second(),
		ch)
	if cacheDirname != "" {
		f = path.Join(cacheDirname, f)
	}
	return f
}

func tempNameFromChannel(ch channel.Channel, t time.Time) string {
	return fileNameFromChannel(ch, t) + "~"
}

func createOneParquet(ch channel.Channel, df interface{}, startedAt time.Time, cfg *Config) (*parq, error) {
	tempName := tempNameFromChannel(ch, startedAt)
	fileName := fileNameFromChannel(ch, startedAt)
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
	channels map[channel.Channel]*channelDef
	sync.WaitGroup
}

func (self *Channels) worker(ch channel.Channel, df interface{}, cr chan interface{}, ci chan interface{}, startedAt time.Time, cfg *Config) {

	parq, err := createOneParquet(ch, df, startedAt, cfg)
	indexCloseCount := 0

	if err != nil {
		logger.Fatal(err.Error())
	} else {

		//logger.Infof("writer started: %s\n", parq.fileName)

		s3t := &S3Tags{
			Exchanges: make(map[int32]int32),
			Pairs:     make(map[int32]int32),
			ChannelNo: int32(ch),
			StartedAt: startedAt.Unix(),
		}

		var timemark map[int32]int64

		for {

			r, ok := <-cr
			if !ok {
				break
			}

			if ch == ChIndex {
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

			if ch != ChIndex {
				s3t.Exchanges[int32(origin)] += 1
				s3t.Pairs[pair.AsInt()] += 1
				s3t.Count += 1
				if err := parq.writer.Write(r); err != nil {
					logger.Fatal(err.Error())
				}
				ci <- &indexRecord{
					record{origin, pair},
					int32(ch),
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

		if ch != ChIndex {
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
	go self.worker(ChIndex, &indexRecord{}, ci, nil, t, cfg)
}

type Writer struct {
	*Config
	cClose chan struct{}
	done   sync.WaitGroup
}

func (self *Writer) worker() {

	var ch = Channels{
		map[channel.Channel]*channelDef{
			channel.Trade: &channelDef{
				&tradeRecord{},
				nil,
			},
			channel.Candlestick: &channelDef{
				&candleRecord{},
				nil,
			},
			channel.Depth: &channelDef{
				&depthRecord{},
				nil,
			},
			ChSnapshot: &channelDef{
				&snapRecord{},
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
				ch.channels[channel.Trade].cx <- &tradeRecord{
					record:    record{msg.Origin, msg.Pair},
					Origin:    msg.Origin.String(),
					Coin1:     msg.Pair[0].String(),
					Coin2:     msg.Pair[1].String(),
					Price:     msg.Value.Price,
					Qty:       msg.Value.Qty,
					Sell:      msg.Value.Sell,
					Timestamp: msg.Value.Timestamp.UTC().UnixNano() / 1000,
				}

			case *message.Candlestick:
				ch.channels[channel.Candlestick].cx <- &candleRecord{
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

			case *message.Orders:
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
				ch.channels[channel.Depth].cx <- r

			case *SnapshotMsg:
				r := &snapRecord{
					record:      record{msg.Origin, msg.Pair},
					Origin:      msg.Origin.String(),
					Coin1:       msg.Pair[0].String(),
					Coin2:       msg.Pair[1].String(),
					Timestamp:   msg.Timestamp.UTC().UnixNano() / 1000,
					BidsPrice:   make([]float32, len(msg.Bids)),
					BidsQty:     make([]float32, len(msg.Bids)),
					AsksPrice:   make([]float32, len(msg.Asks)),
					AsksQty:     make([]float32, len(msg.Asks)),
					TdPrice:     make([]float32, len(msg.Trades)),
					TdQty:       make([]float32, len(msg.Trades)),
					TdTimestamp: make([]int64, len(msg.Trades)),
				}

				r.fill1(msg.Candles1)
				r.fill10(msg.Candles10)
				r.fill60(msg.Candles60)

				for i, v := range msg.Bids {
					r.BidsPrice[i] = v.Price
					r.BidsQty[i] = v.Qty
				}
				for i, v := range msg.Asks {
					r.AsksPrice[i] = v.Price
					r.AsksQty[i] = v.Qty
				}
				for i, v := range msg.Trades {
					r.TdPrice[i] = v.Price
					r.TdQty[i] = v.Qty
					r.TdTimestamp[i] = v.Timestamp.UTC().UnixNano() / 1000
				}

				//fmt.Println(r)

				ch.channels[ChSnapshot].cx <- r
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
