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

    bc          *bitcask.BitCask
    config      *Config
    htable      map[string]*command
    l           net.Listener
    signal      chan int

    // conn mutex
    connMu      sync.Mutex
    conns       map[*conn]struct{}

    // 40 bytes, hex random run id for different server
    runID       []byte
    repl struct {
        sync.RWMutex
        // as maseter
        slaves map[*conn]chan struct{}

        // as slave
        masterRunId string
        masterConnState int32
        master chan *conn
        masterAddr string

        slaveofReply chan struct{}
        syncFileId  int64
        syncOffset  int64
    }

    counters struct {
        clients         int64
        commands        int64
        commandsFailed  int64
        syncTotalBytes  int64
        syncFull        int64
        syncPartialOK   int64
        syncPartialErr  int64
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

    server.repl.master = make(chan *conn, 0)
    server.repl.slaveofReply = make(chan struct{}, 1)

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
    closeConns()
}

func (s *Server) removeConn(c *conn) {
    s.mu.Lock()
    defer s.mu.Unlock()
    delete(s.conns, c)
}

func (s *Server) addConn(c *conn) {
    s.mu.Lock()
    defer s.mu.Unlock()
    s.conns[c] = struct{}{}
}

func (s *Server) closeConns() {
    s.mu.Lock()
    defer s.mu.Unlock()
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

