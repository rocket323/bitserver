package bitserver

import (
    "os"
    "strconv"
    "fmt"
    "io"
    "log"
    "time"
    redis "github.com/reborndb/go/redis/resp"
)

func (s *Server) initReplication() error {
    s.repl.Lock()
    defer s.repl.Unlock()
    s.repl.slaves = make(map[*conn]chan struct{})
    s.repl.master = make(chan *conn, 0)
    s.repl.slaveofReply = make(chan struct{}, 1)

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

func BReplConfCmd(c *conn, args [][]byte) (redis.Resp, error) {
    return nil, nil
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

    /*
        if current sync file is deleted,
        rotate to next file
    */
    dataPath := bc.GetDataFilePath(fileId)
    if _, err := os.Stat(dataPath); err != nil {
        log.Printf("data-file[%d] not exists, sync next file", fileId)
        fileId = bc.NextDataFileId(fileId)
        offset = 0
    }

    // sync records in current file to slave
    var reachEOF bool
    for {
        rec, err := bc.RefRecord(fileId, offset)
        if err != nil {
            if err == io.EOF {
                reachEOF = true
            } else {
                return err
            }
            break
        }

        size := rec.Size()
        c.w.WriteString(fmt.Sprintf("$%d\r\n", fileId))
        c.w.WriteString(fmt.Sprintf("$%d\r\n", offset))
        c.w.WriteString(fmt.Sprintf("$%d\r\n", size))
        data, err := rec.Encode()
        if err != nil {
            log.Fatalf("encode record failed, %d %d %d", fileId, offset, size)
            return err
        }
        if len(data) != int(size) {
            log.Fatalf("data_len[%d] != size[%d]", len(data), size)
        }
        _, err = c.w.Write(data)
        if err != nil {
            return err
        }

        offset += size
    }

    if reachEOF && fileId < activeFileId {
        fileId = bc.NextDataFileId(fileId)
        offset = 0
    }

    c.syncFileId = fileId
    c.syncOffset = offset
    return c.w.Flush()
}

func (s *Server) startSlaveReplication(c *conn, args [][]byte) {
    ch := make(chan struct{}, 1)
    mfCh := make(chan int64, 10)
    ch <- struct{}{}

    s.repl.Lock()
    s.repl.slaves[c] = ch
    s.repl.Unlock()

    log.Printf("start sync to slave %s", c)
    go func(c *conn, ch chan struct{}, mfCh chan int64) {
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
    }(c, ch, mfCh)
}

func init() {
    Register("bsync", BSyncCmd, CmdReadOnly)
    Register("breplconf", BReplConfCmd, CmdReadOnly)
}

