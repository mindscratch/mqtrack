package main

import (
	"fmt"
	"regexp"

	"github.com/mindscratch/foobarbaz"
)

func main() {
	// <app name>|destinations comma-delimited|<p or c for publisher or consumer>
	lp := regexp.MustCompile(`^([^|]+)\|(.*)\|([pc])$`)

	srv := &foobarbaz.Server{
		Protocol:               "tcp", //defaultProtocol,
		ServiceAddress:         ":8125",
		MaxTCPConnections:      250,
		TCPKeepAlive:           false,
		LineParser:             lp,
		AllowedPendingMessages: 1000,
		DeleteCounters:         true,
		DeleteGauges:           true,
		DeleteSets:             true,
		DeleteTimings:          true,
	}
	err := srv.Start()
	if err != nil {
		fmt.Printf("ERR: %#v\n", err)
	}
	srv.Wait()
}
