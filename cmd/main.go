package main

import (
    "flag"
    "log"
    "os"
    "os/signal"
    "syscall"
    "github.com/rocket323/bitserver"
)

var (
    listenPort int
    dbpath string
)

func init() {
    flag.IntVar(&listenPort, "l", 12345, "listen port")
    flag.StringVar(&dbpath, "db", "testdb", "db path")
}

func main() {
    flag.Parse()

    log.SetFlags(log.Lshortfile | log.LstdFlags)

    config := bitserver.NewConfig()
    config.Listen = listenPort
    config.Dbpath = dbpath
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

