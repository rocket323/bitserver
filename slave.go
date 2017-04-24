package bitserver

import (
    "strings"
    redis "github.com/reborndb/go/redis/resp"
)

// SLAVEOF host port
func SlaveOfCmd(c *conn, args [][]byte) (redis.Resp, error) {
    if len(args) != 2 {
        return toRespErrorf("len(args) = %d, expect = 2", len(args))
    }
    addr := fmt.Sprintf("%s:%s", string(args[0]), string(args[1]))
    log.Printf("set slave of %s", addr)

    var cc *conn
    var err error
    if strings.ToLower(addr) != "no:one" {
        if cc, err = c.s.replicationConnectMaster(addr); err != nil {
            return toRespError(err)
        }
    }

    select {
    case <-c.s.signal:
        if cc != nil {
            cc.Close()
        }
        return toRespErrorf("sync master has been close")
    case c.s.master <- cc:
        <-c.s.repl.slaveofReply
        return redis.NewString("OK"), nil
    }
}

func (s *Server) replicationConnectMaster(addr string) (*conn, error) {
    nc, err := net.DialTimeout("tcp", addr, time.Second)
    if err != nil {
        return nil, err
    }

    c := newConn(nc, s, 0)
    return c, nil
}

const infinityDelay = 10 * 365 * 24 * 3600 * time.Second

func (s *Server) daemonSyncMaster() {
    retryTimer := time.NewTimer(infinityDelay)
    defer retryTimer.Stop()

    for exit := false; !exit; {
        var c *conn
        select {
        case <-lost:
        case <-s.signal:
        case c = <-s.master:
        case <-retryTimer:
        }
    }
}

func init() {
    Register("slaveof", SlaveofCmd, CmdReadOnly)
}

