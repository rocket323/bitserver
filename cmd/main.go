package main

import (
    "log"
    "github.com/rocket323/bitserver"
)

func main() {
    log.SetFlags(log.Lshortfile | log.LstdFlags)

    config := bitserver.NewConfig()
    server, err := bitserver.NewServer(config)
    if err != nil {
        log.Fatal(err)
    }

    if err := server.Serve(); err != nil {
        log.Fatalf("serve failed, err=%s", err)
    }
}

