package main

import (
	"flag"
	"github.com/google/logger"
	"github.com/sudachen/coin-exchange/exchange"
	"github.com/sudachen/coin-exchange/exchange/apifactory"
	"github.com/sudachen/coin-logger/internal"
)

//var channels = []exchange.Channel{exchange.Trade, exchange.Candlestick}
var channels = []exchange.Channel{exchange.Depth}

func main() {

	flag.Parse()

	cfg, err := internal.LoadConfig(flag.Arg(0))
	if err != nil {
		internal.Fail("failed to read config: %v\n", err)
	}

	defer internal.SetupLogger(cfg).Close()
	//logger.SetFlags(log.Ldate|log.Ltime)
	logger.Infof("starting...")

	for _, ex := range cfg.Exchanges {
		api := apifactory.Get(ex)
		if err := api.Subscribe(cfg.Pairs, channels); err != nil {
			logger.Fatalf("failed to subscribe api %v", ex.String())
		}
	}

	if err := internal.StartWriter(cfg); err != nil {
		logger.Fatal(err.Error())
	}

	logger.Infof("started successful.")
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
