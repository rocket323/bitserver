package bitserver

import (
    "strconv"
    "fmt"
    "os"
    "io"
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
            pingPeriod := time.Duration(1) * time.Second
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
    if len(args) != 3 {
        return toRespErrorf("len(args) = %d, expect = 3", len(args))
    }

    s := c.s
    if (s.isSlave(c)) {
        log.Printf("conn %+v is already my slave", c)
        return nil, nil
    }

    // runId := string(args[0])

    fileId, err := strconv.ParseInt(string(args[1]), 10, 64)
    if err != nil {
        return nil, err
    }
    offset, err := strconv.ParseInt(string(args[2]), 10, 64)
    if err != nil {
        return nil, err
    }
    c.syncFileId = fileId
    c.syncOffset = offset

    activeFileId := s.bc.ActiveFileId()
    for c.syncFileId < activeFileId {
        err := s.syncDataFile(c)
        if err != nil {
            log.Println(err)
            return nil, err
        }
    }

    s.startSlaveReplication(c, args)
    return nil, nil
}

func (s *Server) syncDataFile(c *conn) error {
    fileId := c.syncFileId
    offset := c.syncOffset

    bc := s.bc
    activeFileId := bc.ActiveFileId()

    path := s.bc.GetDataFilePath(fileId)
    f, err := os.Open(path)
    if err != nil {
        log.Println(err)
        return err
    }
    defer f.Close()

    fi, err := f.Stat()
    if err != nil {
        return err
    }

    // check if more data to sync
    if offset >= fi.Size() {
        return nil
    }
    log.Printf("sync dataFile %d %d", fileId, offset)

    _, err = f.Seek(offset, os.SEEK_SET)
    if err != nil {
        return err
    }
    length := fi.Size() - offset

    c.w.WriteString(fmt.Sprintf("$%d\r\n", fileId))
    c.w.WriteString(fmt.Sprintf("$%d\r\n", offset))
    c.w.WriteString(fmt.Sprintf("$%d\r\n", length))

    _, err = io.CopyN(c.w, f, length)
    if err != nil {
        return err
    }

    // update offset
    offset = fi.Size()
    if fileId < activeFileId {
        fileId = bc.NextDataFileId(fileId)
        offset = 0
    }

    c.syncFileId = fileId
    c.syncOffset = offset

    return c.w.Flush()
}

func (s *Server) startSlaveReplication(c *conn, args [][]byte) {
    ch := make(chan struct{}, 1)
    ch <- struct{}{}

    s.repl.Lock()
    s.repl.slaves[c] = ch
    s.repl.Unlock()

    log.Println("start sync to slave %s", c)
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

                err := s.syncDataFile(c)
                if err != nil {
                    log.Printf("sync slave failed, err=%s", err)
                    return
                }
            }
        }
    }(c, ch)
}

func init() {
    Register("bsync", BSyncCmd, CmdReadOnly)
    // Register("breplconf", ReplConfCmd, CmdReadOnly)
}

