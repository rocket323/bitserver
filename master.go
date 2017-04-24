package bitserver

import (
    "log"
    redis "github.com/reborndb/go/redis/resp"
)

func (s *Server) initReplication() error {
    s.repl.Lock()
    defer s.repl.Unlock()

    s.repl.slaves = make(map[*conn]chan struct{})
    go func() {
        for {
            pingPeriod := time.Duration(2) * time.Second
            select {
            case <-s.signal:
                return
            case <-time.After(pingPeriod):
                if err := s.replicationFeedSlaves(); err != nil {
                    log.Printf("ping slaves error - %s", err)
                }
            }
        }
    }()
}

func (s *Server) replicationNotifySlaves() error {
    for _, ch := range s.repl.slaves {
        select {
        case ch <- struct{}{}:
        default:
        }
    }
    return nil
}

// BSYNC runId fileId offset
func BSyncCmd(c *conn, args [][]byte) (redis.Resp, error) {
    s := c.s
    if (s.isSlave(c)) {
        return nil, nil
    }

    s.startSlaveReplication(c, args)
    return nil, nil
}

func (s *Server) startSlaveReplication(c *conn, args [][]byte) (redis.Resp, error) {
    ch := make(chan struct{}, 1)
    ch <- struct{}{}

    s.repl.Lock()
    s.repl.slaves[c] = ch
    s.repl.Unlock()

    go func(c *conn, ch chan struct{}) {
        defer func() {
            s.removeConn(c)
            c.Close()
        }

        for {
            select {
            case <-s.signal:
                return
            case _, ok := <-ch:
                if !ok {
                    return
                }

                log.Printf("sync data to slave[%s]", c)
            }
        }
    }(c, ch)
}

func (s *Server) replicationSyncFile() (int, error) {
}

func init() {
    Register("bsync", BSyncCmd, CmdReadOnly)
    Register("breplconf", ReplConfCmd, CmdReadOnly)
}

