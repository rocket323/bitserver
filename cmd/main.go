package main

import (
    "log"
    "os"
    "os/signal"
    "syscall"
    "github.com/rocket323/bitserver"
)

func main() {
    log.SetFlags(log.Lshortfile | log.LstdFlags)

    config := bitserver.NewConfig()
    server, err := bitserver.NewServer(config)
    if err != nil {
        log.Fatal(err)
    }

    c := make(chan os.Signal, 1)
    signal.Notify(c, syscall.SIGTERM, os.Interrupt, os.Kill)

    go func(s *bitserver.Server) {
        for _ = range c {
            log.Println("interrupt and shutdown")
            s.Close()
            os.Exit(0)
        }
    }(server)

    if err := server.Serve(); err != nil {
        log.Fatalf("serve failed, err=%s", err)
    }
}
