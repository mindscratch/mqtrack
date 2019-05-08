package main

import (
	"log"

	"github.com/mindscratch/mqtrack"
)

func main() {
	client, err := mqtrack.NewClient("localhost:8125", "myapp", []string{"foo", "bar", "baz"}, true)
	if err != nil {
		log.Fatal(err)
	}
	defer client.Close()

	err = client.Connect()
	if err != nil {
		log.Fatal(err)
	}

	client.Record()
	if err != nil {
		log.Fatal(err)
	}
}
