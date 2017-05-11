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
            log.Printf("replication connection from master %s was broken, try reconnect 1s later", s.repl.masterAddr.Get())
            retryTimer.Reset(time.Second)
            continue LOOP
        case <-s.signal:
            exit = true
        case c = <-s.repl.master:
            needSlaveOfReply = true
        case <-retryTimer.C:
            log.Printf("try reconnect to master %s", s.repl.masterAddr.Get())
            c, err = s.replicationConnectMaster(s.repl.masterAddr.Get())
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
            s.repl.masterAddr.Set(masterAddr)
            activeFileId := s.bc.ActiveFileId()
            path := s.bc.GetDataFilePath(activeFileId)

            go func(activeFileId int64, path string) {
                defer func() {
                    lost <- 0
                }()
                defer c.Close()
                err := s.bsync(c, activeFileId, path)
                log.Printf("slave %s do bsync err - %s", c, err)
            }(activeFileId, path)
            log.Printf("slaveof %s", s.repl.masterAddr.Get())
        } else {
            s.repl.masterAddr.Set("")
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
        return 0, fmt.Errorf("invalid number, resp = %s", line)
    }
    n, err := strconv.ParseInt(string(line[1:]), 10, 64)
    if err != nil {
        return 0, fmt.Errorf("invalid number, resp = %s, err = %s", line, err)
    }
    return n, nil
}

func (s *Server) bsync(c *conn, activeFileId int64, path string) error {
    // send bsync command
    deadline := time.Now().Add(time.Second * 5)
    if err := c.nc.SetWriteDeadline(deadline); err != nil {
        log.Println(err)
        return err
    }

    fi, err := os.Stat(path)
    if err != nil {
        return err
    }
    offset := fi.Size()

    if err := c.writeRESP(redis.NewRequest("BSYNC", "", activeFileId, offset)); err != nil {
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
    data := make([]byte, int(length))
    var n int
    if length > 0 {
        n, err = io.ReadFull(c.r, data)

        if err != nil || n < int(length) {
            log.Fatalf("n[%d] < length[%d]", n, length)
            return err
        }
    }

    err = s.bc.SyncFile(fileId, offset, length, data)
    if err != nil {
        log.Println(err)
        return err
    }
    return nil
}

func init() {
    Register("slaveof", SlaveOfCmd, CmdReadOnly)
}

