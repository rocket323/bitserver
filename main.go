package main

import (
    "fmt"
    "bitserver"
)

func main() {
    server, err := bitserver.NewServer()
    if err != nil {
        log.Fatal(err)
    }

    if err := server.Serve(); err != nil {
        log.Fatalf("serve failed, err=%s", err)
    }
}

