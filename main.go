package main

import (
	"flag"
	"github.com/google/logger"
	"github.com/sudachen/coin-exchange/exchange"
	"github.com/sudachen/coin-exchange/exchange/apifactory"
	"github.com/sudachen/coin-logger/internal"
	"log"
	"sync"
	"time"
)

func main() {

	flag.Parse()

	cfg, err := internal.LoadConfig(flag.Arg(0))
	if err != nil {
		internal.Fatalf("failed to read config: %v\n", err)
	}

	defer internal.SetupLogger(cfg).Close()
	logger.SetFlags(log.Ldate | log.Ltime)
	logger.Infof("starting...")

	for _, ex := range cfg.Exchanges {
		api := apifactory.Get(ex)
		if err := api.Subscribe(cfg.Pairs, []exchange.Channel{exchange.Trade, exchange.Candlestick}); err != nil {
			logger.Fatalf("failed to subscribe api %v", ex.String())
		}
		if err := api.Subscribe(cfg.Pairs, []exchange.Channel{exchange.Depth}); err != nil {
			logger.Fatalf("failed to subscribe api %v", ex.String())
		}
	}

	if err := internal.StartWriter(cfg); err != nil {
		logger.Fatal(err.Error())
	}

	logger.Rinfo("started, waiting for Ctrl-C")
	err = internal.WaitForCtrlC()
	logger.Info("stopping...")

	wg := sync.WaitGroup{}
	for _, ex := range cfg.Exchanges {
		wg.Add(1)
		go func() {
			api := apifactory.Get(ex)
			if err := api.UnsubscribeAll(time.Second * 10); err != nil {
				logger.Errorf("on shutdown: %v", err.Error())
			}
			wg.Done()
		}()
	}
	wg.Wait()

	internal.StopWriter()
	internal.WaitForUploads()

	if err != nil {
		logger.Fatal(err.Error())
	}

	logger.Rinfo("stopped successful")
}
