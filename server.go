package bitserver

import (
    "sync"
    "net"
    "fmt"
    "log"
    "github.com/rocket323/bitcask"
    redis "github.com/reborndb/go/redis/resp"
)

type Server struct {
    mu          sync.Mutex
    closed      bool

    bc          *bitcask.BitCask
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

    l, err := net.Listen("tcp", "0.0.0.0:12345")
    if err != nil {
        log.Fatalf("listen failed, err=%s", err)
    }

    server := &Server{
        closed: false,
        bc: bc,
        config: c,
        htable: globalCommand,
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
                c := newConn(nc, s, 2000)
                log.Printf("new connection: %s", c)

                if err := c.serve(); err != nil {
                    log.Printf("connection lost: %s\n", err)
                }
            }()
        }
    }
    return nil
}

func (s *Server) Close() {
    s.mu.Lock()
    defer s.mu.Unlock()

    if s.closed {
        return
    }
    s.closed = true
}

func (s *Server) removeConn(c *conn) {
}

func (s *Server) addConn(c *conn) {
}

func toRespError(err error) (redis.Resp, error) {
    return redis.NewError(err), err
}

func toRespErrorf(format string, args ...interface{}) (redis.Resp, error) {
    err := fmt.Errorf(format, args...)
    return toRespError(err)
}

