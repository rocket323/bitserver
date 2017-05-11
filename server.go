package bitserver

import (
    "sync"
    "net"
    "fmt"
    "log"
    "github.com/rocket323/bitcask"

    "github.com/reborndb/go/atomic2"
    redis "github.com/reborndb/go/redis/resp"
)

type Server struct {
    mu          sync.Mutex
    runID       []byte

    bc          *bitcask.BitCask
    config      *Config
    htable      map[string]*command
    l           net.Listener
    signal      chan int

    // conn mutex
    connMu      sync.Mutex
    conns       map[*conn]struct{}

    repl struct {
        sync.RWMutex
        // as maseter
        slaves map[*conn]chan struct{}

        // as slave
        masterRunId string
        masterAddr atomic2.String
        masterConnState atomic2.String
        master chan *conn
        slaveofReply chan struct{}
        syncFileId  int64
        syncOffset  int64
    }

    counters struct {
        clients         atomic2.Int64
        commands        atomic2.Int64
        commandsFailed  atomic2.Int64
        syncTotalBytes  atomic2.Int64
        syncFull        atomic2.Int64
        syncPartialOK   atomic2.Int64
        syncPartialErr  atomic2.Int64
    }
}

func NewServer(c *Config) (*Server, error) {
    opts := bitcask.NewOptions()
    bc, err := bitcask.Open(c.Dbpath, opts)
    if err != nil {
        log.Fatal(err)
    }

    addr := fmt.Sprintf("0.0.0.0:%d", c.Listen)
    l, err := net.Listen("tcp", addr)
    if err != nil {
        log.Fatalf("listen failed, err=%s", err)
    }

    server := &Server{
        bc: bc,
        config: c,
        htable: globalCommand,
        signal: make(chan int, 0),
        conns: make(map[*conn]struct{}),
        l: l,
    }

    if err := server.initReplication(); err != nil {
        server.Close()
        return nil, err
    }

    go server.daemonSyncMaster()
    return server, nil
}

func (s *Server) Serve() error {
    log.Printf("listen on %d\ndbpath: %s", s.config.Listen, s.config.Dbpath)
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

func (s *Server) merge() error {
    done := make(chan int, 1)
    s.bc.Merge(done)
    return nil
}

func (s *Server) isSlave(c *conn) bool {
    s.repl.Lock()
    defer s.repl.Unlock()
    _, ok := s.repl.slaves[c]
    return ok
}

func (s *Server) Close() {
    s.mu.Lock()
    defer s.mu.Unlock()

    s.bc.Close()
    s.closeConns()
}

func (s *Server) removeConn(c *conn) {
    s.connMu.Lock()
    defer s.connMu.Unlock()
    delete(s.conns, c)
}

func (s *Server) addConn(c *conn) {
    s.connMu.Lock()
    defer s.connMu.Unlock()
    s.conns[c] = struct{}{}
}

func (s *Server) closeConns() {
    s.connMu.Lock()
    defer s.connMu.Unlock()
    for c, _ := range s.conns {
        c.Close()
    }
    s.conns = make(map[*conn]struct{})
}

func toRespError(err error) (redis.Resp, error) {
    return redis.NewError(err), err
}

func toRespErrorf(format string, args ...interface{}) (redis.Resp, error) {
    err := fmt.Errorf(format, args...)
    return toRespError(err)
}

