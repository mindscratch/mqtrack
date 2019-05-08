package main

import (
	"fmt"
	"log"
	"os"
	"os/signal"
	"syscall"

	"github.com/mindscratch/mqtrack"
)

func main() {
	sigCh := make(chan os.Signal)
	signal.Notify(sigCh, os.Interrupt, syscall.SIGTERM)

	srv := mqtrack.NewUDPServer()

	go func() {
		signalType := <-sigCh
		signal.Stop(sigCh)

		log.Println("received signal, type:", signalType)

		srv.Stop()
		os.Exit(0)
	}()

	err := srv.Start()
	if err != nil {
		fmt.Printf("ERR: %#v\n", err)
	}
	srv.Wait()
}
