package bitserver

import (
    "io"
    "os"
    "strconv"
    "strings"
    "time"
    "fmt"
    "log"
    "net"
    redis "github.com/reborndb/go/redis/resp"
)

const (
    masterConnNone = 0              // no replication
    masterConnConnect = 1           // must connect master
    masterConnConnecting = 2        // connecting to master
    masterConnSyncing = 3           // syncing to master
    masterConnConnected = 4         // connected to master
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
    case c.s.repl.master <- cc:
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
    var last *conn
    lost := make(chan int, 0)
    retryTimer := time.NewTimer(infinityDelay)
    defer retryTimer.Stop()

    var err error
LOOP:
    for exit := false; !exit; {
        var c *conn
        needSlaveOfReply := false
        select {
        case <-lost:
            // here means replication conn was broken, we will reconnect it
            last = nil
            log.Printf("replication connection from master %s was broken, try reconnect 1s later", s.repl.masterAddr)
            retryTimer.Reset(time.Second)
            continue LOOP
        case <-s.signal:
            exit = true
        case c = <-s.repl.master:
            log.Printf("get master conn %+v", c)
            needSlaveOfReply = true
        case <-retryTimer.C:
            log.Printf("try reconnect to master %s", s.repl.masterAddr)
            c, err = s.replicationConnectMaster(s.repl.masterAddr)
            if err != nil {
                log.Printf("replication reconnect to master %s failed, try 1s laster again -%s", s.repl.masterAddr, err)
                retryTimer.Reset(time.Second)
                continue LOOP
            }
        }
        retryTimer.Reset(infinityDelay)

        if last != nil {
            last.Close()
            // TODO wait last connection lost
            <-lost
        }
        last = c

        if c != nil {
            masterAddr := c.nc.RemoteAddr().String()
            s.repl.masterAddr = masterAddr

            go func() {
                defer func() {
                    lost <- 0
                }()
                defer c.Close()
                err := s.bsync(c)
                log.Printf("slave %s do bsync err - %s", c, err)
            }()
            log.Printf("slaveof %s", s.repl.masterAddr)
        } else {
            log.Printf("slaveof no one")
        }

        if needSlaveOfReply {
            s.repl.slaveofReply <- struct{}{}
        }
    }
}

func readInt(c *conn) (int64, error) {
    line, err := c.readLine()
    if err != nil {
        return 0, err
    }
    if line[0] != '$' {
        return 0, fmt.Errorf("invalid number, rsp = %s", line)
    }
    n, err := strconv.ParseInt(string(line[1:]), 10, 64)
    if err != nil {
        return 0, fmt.Errorf("invalid number, rsp = %s, err = %s", line, err)
    }
    return n, nil
}

func (s *Server) bsync(c *conn) error {
    // send bsync command
    deadline := time.Now().Add(time.Second * 5)
    if err := c.nc.SetWriteDeadline(deadline); err != nil {
        log.Println(err)
        return err
    }
    if err := c.writeRESP(redis.NewRequest("BSYNC", "", "")); err != nil {
        log.Println(err)
        return err
    }

    log.Printf("start sync from master")
    // sync data files
    for {
        err := s.syncFromMaster(c)
        if err != nil {
            log.Printf("sync file from master failed, err = %s", err)
            return err
        }
    }
    return nil
}

func (s *Server) syncFromMaster(c *conn) error {
    fileId, err := readInt(c)
    if err != nil {
        return err
    }
    offset, err := readInt(c)
    if err != nil {
        return err
    }
    length, err := readInt(c)
    if err != nil {
        return err
    }
    log.Printf("sync fileId[%d], offset[%d], length[%d]", fileId, offset, length)

    path := s.bc.GetDataFilePath(fileId)
    f, err := os.OpenFile(path, os.O_WRONLY | os.O_CREATE, 0644)
    if err != nil {
        return err
    }
    defer f.Close()

    _, err = f.Seek(offset, os.SEEK_SET)
    if err != nil {
        return err
    }

    _, err = io.CopyN(f, c.r, length)
    if err != nil {
        return err
    }
    return nil
}

func init() {
    Register("slaveof", SlaveOfCmd, CmdReadOnly)
}

