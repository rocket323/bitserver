package bitserver

import (
    "log"
    "time"
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
                if err := s.replicationNotifySlaves(); err != nil {
                    log.Printf("ping slaves error - %s", err)
                }
            }
        }
    }()
    return nil
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
        log.Printf("conn %+v is already my slave", c)
        return nil, nil
    }

    s.startSlaveReplication(c, args)
    return nil, nil
}

func (s *Server) startSlaveReplication(c *conn, args [][]byte) {
    ch := make(chan struct{}, 1)
    ch <- struct{}{}

    s.repl.Lock()
    s.repl.slaves[c] = ch
    s.repl.Unlock()

    go func(c *conn, ch chan struct{}) {
        defer func() {
            s.removeConn(c)
            c.Close()
        }()

        for {
            select {
            case <-s.signal:
                return
            case _, ok := <-ch:
                if !ok {
                    return
                }

                err := s.replicationSyncFile(c)
                if err != nil {
                    log.Printf("sync slave failed, err=%s", err)
                    return
                }
                log.Printf("sync data to slave[%s]", c)
            }
        }
    }(c, ch)
}

func (s *Server) replicationSyncFile(c *conn) error {
    log.Printf("ping slave %s", c)

    if err := c.writeRESP(redis.NewRequest("PING")); err != nil {
        return err
    }
    return nil
}

func init() {
    Register("bsync", BSyncCmd, CmdReadOnly)
    // Register("breplconf", ReplConfCmd, CmdReadOnly)
}

