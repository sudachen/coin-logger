package main

import (
	"flag"
	"github.com/google/logger"
	"github.com/sudachen/coin-exchange/exchange"
	"github.com/sudachen/coin-exchange/exchange/apifactory"
	"github.com/sudachen/coin-logger/internal"
)

var channels = []exchange.Channel{exchange.Trade, exchange.Candlestick}

func main() {

	flag.Parse()

	cfg, err := internal.LoadConfig(flag.Arg(0))
	if err != nil {
		internal.Fail("failed to read config: %v\n", err)
	}

	defer internal.SetupLogger(cfg).Close()

	logger.Infof("starting...")

	for _, ex := range cfg.Exchanges {
		api := apifactory.Get(ex)
		for _, c := range channels {
			pairs := api.FilterSupported(cfg.Pairs)
			if err := api.SubscribeCombined(pairs, c); err != nil {
				for _, pair := range cfg.Pairs {
					if err := api.Subscribe(pair, c); err != nil {
						logger.Fatalf("failed to subscribe pair %v/%v for %v: %v",
							pair[0], pair[1], ex, err)
					}
				}
			}
		}
	}

	if err := internal.StartWriter(cfg); err != nil {
		logger.Fatal(err.Error())
	}

	logger.Infof("processing...")
	err = internal.WaitForCtrlC()

	for _, ex := range cfg.Exchanges {
		api := apifactory.Get(ex)
		_ = api.UnsubscribeAll()
	}

	logger.Info("stopping...")
	internal.StopWriter()
	internal.WaitForUploads()

	if err != nil {
		logger.Fatal(err.Error())
	}

	logger.Info("stopped successful")
}
