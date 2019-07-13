package internal

import (
	"os"
	"os/signal"
)

var errChannel = make(chan error, 10)

func WaitForCtrlC() error {
	var signal_channel chan os.Signal
	signal_channel = make(chan os.Signal, 1)
	signal.Notify(signal_channel, os.Interrupt)
	select {
	case err := <-errChannel:
		return err
	case <-signal_channel:
		return nil
	}
}
