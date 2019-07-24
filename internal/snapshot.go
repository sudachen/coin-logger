package internal

import (
	"github.com/google/logger"
	"github.com/sudachen/coin-exchange/exchange"
	"github.com/sudachen/coin-exchange/exchange/apifactory"
	"github.com/sudachen/coin-exchange/exchange/message"
	"strings"
	"sync"
	"time"
)

type Snapshots struct {
	*Config
	cClose chan struct{}
	done   sync.WaitGroup
}

func (sn *Snapshots) Schedule(exchange exchange.Exchange, pairs []exchange.CoinPair) error {
	if sn.cClose == nil {
		sn.cClose = make(chan struct{})
	}
	sn.done.Add(1)
	go sn.worker(exchange, pairs)
	return nil
}

func (sn *Snapshots) Stop(timeout time.Duration, wg *sync.WaitGroup) {
	c := make(chan struct{})
	close(sn.cClose)
	go func() { sn.done.Wait(); close(c) }()

	f := func() {
		defer func() {if wg!= nil {wg.Done()} } ()
		for {
			select {
			case <-sn.cClose:
				return
			case <-time.After(timeout):
				logger.Warning("timed out: snapshots not finished yet")
				return
			}
		}
	}

	if wg == nil {
		f()
	} else {
		wg.Add(1)
		go f()
	}
}

type SnapshotMsg struct {
	Origin exchange.Exchange
	Pair   exchange.CoinPair

	Timestamp time.Time

	Bids   []message.OrderValue
	Asks   []message.OrderValue
	Trades []message.TradeValue

	Candles1  []message.Kline
	Candles10 []message.Kline
	Candles60 []message.Kline
}

func stringify(ex exchange.Exchange, pairs []exchange.CoinPair) string {
	const maxPairsCountInString = 3
	var ss1 []string

	for i, v := range pairs {
		if i < maxPairsCountInString {
			ss1 = append(ss1, v.String())
		} else if i == maxPairsCountInString {
			ss1 = append(ss1, "...")
			break
		}
	}

	return "Snapshot{" + ex.String() + "|" + strings.Join(ss1, ",") + "}"
}

func (sn *Snapshots) worker(ex exchange.Exchange, pairs []exchange.CoinPair) {
	ticker := time.NewTicker(10*time.Minute)
	//ticker := time.NewTicker(10*time.Second)
	pairs = apifactory.Get(ex).FilterSupported(pairs)
	name := stringify(ex, pairs)
	logger.Infof("snapshoter started %v",name)

	loop:for {
		select {
		case <-sn.cClose:
			ticker.Stop()
			break loop
		case <-ticker.C:
			sn.done.Add(1)
			go func() {
				pair_loop:for _, pair := range pairs {
					select {
					case <-sn.cClose:
						break pair_loop
					default:
						gw := sync.WaitGroup{}
						mesg := SnapshotMsg{Origin: ex, Pair: pair, Timestamp: time.Now()}
						if q, err := apifactory.Get(ex).Queries(pair); err != nil {
							// fmt.Println(err)
							// unsupported pair
						} else if q != nil {
							gw.Add(1)
							go func() {
								defer gw.Done()
								if m, err := q.QueryDepth(100); err != nil {
									logger.Errorf("depth snapshot: %v", err.Error())
								} else {
									mesg.Asks = m.Asks
									mesg.Bids = m.Bids
								}
							}()

							gw.Add(1)
							go func() {
								defer gw.Done()
								if m, err := q.QueryTrades(100); err != nil {
									logger.Errorf("trades snapshot: %v", err.Error())
								} else {
									mesg.Trades = m.Values
								}
							}()

							gw.Add(1)
							go func() {
								defer gw.Done()
								if m, err := q.QueryCandlesticks(1, 15); err != nil {
									logger.Errorf("candlestick/1m snapshot: %v", err.Error())
								} else {
									mesg.Candles1 = m.Klines
								}
							}()

							gw.Add(1)
							go func() {
								defer gw.Done()
								if m, err := q.QueryCandlesticks(15, 12); err != nil {
									logger.Errorf("candlestick/15m snapshot: %v", err.Error())
								} else {
									mesg.Candles10 = m.Klines
								}
							}()

							gw.Add(1)
							go func() {
								defer gw.Done()
								if m, err := q.QueryCandlesticks(60, 24); err != nil {
									logger.Errorf("candlestick/1h snapshot: %v", err.Error())
								} else {
									mesg.Candles60 = m.Klines
								}
							}()

							gw.Wait()
							if mesg.Candles1 != nil &&
								mesg.Candles10 != nil &&
								mesg.Candles60 != nil &&
								mesg.Asks != nil &&
								mesg.Bids != nil &&
								mesg.Trades != nil {

								exchange.Collector.Messages <- &mesg
							}
						}
					}
				}
				sn.done.Done()
			}()
		}
	}

	logger.Infof("snapshoter stopped %v",name)
	sn.done.Done()
}
