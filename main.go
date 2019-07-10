package main

import (
	"fmt"
	"github.com/sudachen/coin-exchange/exchange"
	"github.com/sudachen/coin-exchange/exchange/binance"
)

func main() {

	_ = binance.Websocket(
		exchange.CoinPair{exchange.BTC,exchange.USD},
		exchange.Candlestick).
		Subscribe()

	_ = binance.Websocket(
		exchange.CoinPair{exchange.BTC,exchange.USD},
		exchange.Trade).
		Subscribe()

	fmt.Println("waiting for messages")

	for e := range exchange.Collector.MsgStream {
		fmt.Printf("%#v\n", e)
	}
}
