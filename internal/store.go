package internal

import (
	"fmt"
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
	pfx string
	df interface{}
	cx chan interface{}
	close chan struct{}
}

type parq struct{
	name 		string
	startedAt 	time.Time
	source 		source.ParquetFile
	writer 		*writer.ParquetWriter
}

var channels = map[exchange.Channel]*channelDef{
	exchange.Trade: &channelDef{
		"trade-history",
		&tradeRecord{},
		make(chan interface{}, 100),
		nil,
	},
	exchange.Candlestick: &channelDef{
		"candles-history",
		&candleRecord{},
		make(chan interface{}, 100),
		nil,
	},
}

type tradeRecord struct {
	Origin 			string 		`parquet:"name=origin, type=UTF8, encoding=PLAIN_DICTIONARY"`
	Coin1  			string 		`parquet:"name=coin1, type=UTF8, encoding=PLAIN_DICTIONARY"`
	Coin2  			string 		`parquet:"name=coin2, type=UTF8, encoding=PLAIN_DICTIONARY"`
	TradeId	 		int64		`parquet:"name=trade_id, type=INT64"`
	Price   		float32		`parquet:"name=price, type=FLOAT"`
	Qty     		float32		`parquet:"name=qty, type=FLOAT"`
	BuyerOrderId   	int64		`parquet:"name=bayer_order_id, type=INT64"`
	SellerOrderId  	int64		`parquet:"name=seller_order_id, type=INT64"`
	TradeOrderTime 	int64		`parquet:"name=trader_time, type=TIMESTAMP_MICROS"`
}

type candleRecord struct {
	Origin 			string 		`parquet:"name=origin, type=UTF8, encoding=PLAIN_DICTIONARY"`
	Coin1  			string 		`parquet:"name=coin1, type=UTF8, encoding=PLAIN_DICTIONARY"`
	Coin2  			string 		`parquet:"name=coin2, type=UTF8, encoding=PLAIN_DICTIONARY"`
	StartTime 		int64		`parquet:"name=start_time, type=TIMESTAMP_MICROS"`
	EndTime 		int64		`parquet:"name=end_time, type=TIMESTAMP_MICROS"`
	Interval  		int32		`parquet:"name=interval, type=INT32"`
	TradeNum  		int32		`parquet:"name=tradenum, type=INT32"`
	FirstTradeId 	int64		`parquet:"name=first_trade_id, type=INT64"`
	LastTradeId  	int64		`parquet:"name=last_trade_id, type=INT64"`
	Open   			float32     `parquet:"name=open, type=FLOAT"`
	Close  			float32		`parquet:"name=close, type=FLOAT"`
	High   			float32		`parquet:"name=high, type=FLOAT"`
	Low    			float32		`parquet:"name=low, type=FLOAT"`
	Volume 			float32		`parquet:"name=volume, type=FLOAT"`
}

func cacheDir(cfg *Config) (*string, error){
	dirname := cfg.S3.Cache
	if dir1,_ := path.Split(dirname); dir1 == "" {
		s3cache, ok := os.LookupEnv("S3_CACHE")
		if ok {
			dirname = path.Join(s3cache,dirname)
		}
	}
	if st, err := os.Stat(dirname); err != nil {
		if err = os.MkdirAll(dirname,0700); err != nil {
			return nil, fmt.Errorf("can't create cache directory %v", dirname)
		}
	} else if !st.IsDir() {
		return nil, fmt.Errorf("is not directory: %v", dirname)
	}
	return &dirname, nil
}

func fileNameFromChannel(channel exchange.Channel, t time.Time) string {
	utc := t.UTC()
	f := fmt.Sprintf("%v-%04d%02d%02dT%02d%02d%02d.parquet",
		channels[channel].pfx,
		utc.Year(), utc.Month(), utc.Day(),
		utc.Hour(), utc.Minute(), utc.Second())
	if cacheDirname != "" {
		f = path.Join(cacheDirname,f)
	}
	return f
}

func createOneParquet(channel exchange.Channel, startedAt time.Time, cfg *Config) (*parq, error) {
	name := fileNameFromChannel(channel, startedAt)
	if fw, err := local.NewLocalFileWriter(name); err != nil {
		fmt.Println("Can't create local file", err)
		return nil, err
	} else {
		if pw, err := writer.NewParquetWriter(fw, channels[channel].df, 4); err != nil {
			fmt.Println("Can't create parquet writer", err)
			return nil, err
		} else {
			return &parq{ name, startedAt, fw, pw }, nil
		}
	}
}

func worker1(channel exchange.Channel, startedAt time.Time, cfg *Config) {
	done := false
	c := make(chan struct{},1)
	channels[channel].close = c
	parq, err := createOneParquet(channel, startedAt, cfg);
	if err != nil {
		Fail(err.Error())
	} else {
		for !done {
			select {
			case <-c:
				close(c)
				_ = parq.writer.WriteStop()
				_ = parq.source.Close()
				done = true
			case r := <-channels[channel].cx:
				if err := parq.writer.Write(r); err != nil {
					Fail(err.Error())
				}
			}
		}
	}
	if parq != nil {
		fmt.Fprintf(os.Stderr,"worker for %v exited\n",parq.name)
	}
	waitGroup.Done()
}

func closeStore() {
	for _,v := range channels{
		if v.close != nil {
			v.close <- struct{}{}
		}
	}
	waitGroup.Wait()
}

func openStore(cfg *Config) {
	closeStore()
	t := time.Now()
	for c,_ := range channels{
		waitGroup.Add(1)
		go worker1(c, t, cfg)
	}
}

var writerClose chan struct{}
var writerDone sync.WaitGroup

func Writer(cfg *Config) {
	writerClose = make(chan struct{})
	openStore(cfg)
	for {
		select {
		case <-writerClose:
			closeStore()
			close(writerClose)
			writerDone.Done()
			return
		case e := <-exchange.Collector.MsgStream:
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
					TradeOrderTime: msg.TradeOrderTime.UnixNano()/1000,
				}
			case *message.Candlestick:
				channels[exchange.Candlestick].cx <- &candleRecord{
					Origin:         msg.Origin.String(),
					Coin1:          msg.Pair[0].String(),
					Coin2:          msg.Pair[1].String(),
					StartTime:		msg.StartTime.UnixNano()/1000,
					EndTime: 		msg.EndTime.UnixNano()/1000,
					Interval: 		msg.Interval,
					TradeNum:  		msg.TradeNum,
					FirstTradeId: 	msg.FirstTradeId,
					LastTradeId:	msg.LastTradeId,
					Open:			msg.Open,
					Close:  		msg.Close,
					High:			msg.High,
					Low:			msg.Low,
					Volume:			msg.Volume,
				}
			}
		}
	}
}

func StartWriter(cfg *Config) error{
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
	writerDone.Wait()
	fmt.Fprintf(os.Stderr,"exited\n")
}
