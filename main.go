package main

import (
	"flag"
	"github.com/google/logger"
	"github.com/sudachen/coin-exchange/exchange/apifactory"
	"github.com/sudachen/coin-exchange/exchange/channel"
	"github.com/sudachen/coin-logger/internal"
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
	//logger.SetFlags(log.Ldate | log.Ltime)
	logger.Infof("starting...")

	for _, ex := range cfg.Exchanges {
		api := apifactory.Get(ex)
		if err := api.Subscribe(cfg.Pairs, channel.Trade, channel.Candlestick); err != nil {
			logger.Fatalf("failed to subscribe api %v", ex.String())
		}
		if err := api.Subscribe(cfg.Pairs, channel.Depth); err != nil {
			logger.Fatalf("failed to subscribe api %v", ex.String())
		}
	}

	sn := &internal.Snapshots{Config: cfg}
	for _, ex := range cfg.Exchanges {
		if err := sn.Schedule(ex, cfg.Pairs); err != nil {
			logger.Fatalf("failed to schedule snapshots on %v", ex.String())
		}
	}

	wr := &internal.Writer{Config: cfg}
	if err := wr.Start(); err != nil {
		logger.Fatal(err.Error())
	}

	logger.Rinfo("started, waiting for Ctrl-C")
	internal.WaitForCtrlC()
	logger.Info("stopping...")

	wg := sync.WaitGroup{}
	sn.Stop(5*time.Second, &wg)
	apifactory.UnsubscribeAll(5*time.Second, &wg)
	wg.Wait()
	wr.Stop()

	logger.Rinfo("stopped successful")
	logger.Info("exited")
}
