package bitserver

import (
    "sync"
    "net"
    "log"
    "bitcask"
)

type Server struct {
    mu          sync.Mutex
    closed      bool

    bc          *bitcask.Bitcask
    config      *Config
    htable      map[string]*command
    l           net.Listener
    signal      chan int

    // conn mutex
    connMu      sync.Mutex
    conns       map[*conn]struct{}
}

func NewServer(c *Config) (*Server, error) {

    opts := bitcask.NewOptions()
    bc, err := bitcask.Open("testdb", opts)
    if err != nil {
        log.Fatal(err)
    }

    l, err := net.Listen("tcp", 12345)
    if err != nil {
        log.Fatalf("listen failed, err=%s", err)
    }

    server := &Server{
        closed: false,
        bc: bc,
        config: config,
        htable: make(map[string]*command),
        signal: make(chan int, 0),
        conns: make(map[*conn]struct{}),
        l: l,
    }

    return server, nil
}

func (s *Server) Serve() error {
    for {
        if nc, err := s.l.Accept(); err != nil {
            log.Println(err)
        } else {
            go func() {
                c := newConn(nc, s)
                log.Printf("new connection: %s", c)

                if err := c.serve(s); err != nil {
                    log.Println("connection lost: %s", err)
                }
            }()
        }
    }
    return nil
}

func (s *Server) Close() error {
    s.mu.Lock()
    defer s.mu.Unlock()

    if s.closed {
        return
    }
    s.closed = true
}

