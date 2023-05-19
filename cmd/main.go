package main

import (
	"os"
	"os/signal"
	"syscall"
)

func main() {

	done := make(chan struct{}, 1)
	go func() {
		sigChan := make(chan os.Signal, 1)
		signal.Notify(sigChan, syscall.SIGINT)
		<-sigChan
		done <- struct{}{}
	}()

	// create server struct

	// listen

	///go listen

	<-done
	// cleanup

}
