package main

import (
	"github.com/sudachen/coin-exchange/exchange"
	"github.com/sudachen/coin-exchange/exchange/apifactory"
	"github.com/sudachen/coin-logger/internal"
)

var channels = []exchange.Channel{exchange.Trade, exchange.Candlestick}

func main() {

	cfg, err := internal.LoadConfig("")
	if err != nil {
		internal.Fail("failed to read config: %v\n", err)
	}

	for _, ex := range cfg.Exchanges {
		api := apifactory.Get(ex)

		for _, c := range channels {
			pairs := api.FilterSupported(cfg.Pairs)
			if err := api.SubscribeCombined(pairs, c); err != nil {
				for _, pair := range cfg.Pairs {
					if err := api.Subscribe(pair, c); err != nil {
						internal.Fail("failed to subscribe pair %v/%v for %v: %v",
							pair[0], pair[1], ex, err)
					}
				}
			}
		}
	}

	if err := internal.StartWriter(cfg); err != nil {
		internal.Fail(err.Error())
	}

	err = internal.WaitForCtrlC()

	for _, ex := range cfg.Exchanges {
		api := apifactory.Get(ex)
		_ = api.UnsubscribeAll()
	}

	internal.StopWriter()

	if err != nil {
		internal.Fail(err.Error())
	}
}
