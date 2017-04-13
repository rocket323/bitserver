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
}

func (s *Server) Serve() error {
}

func (s *Server) Close() error {
}

