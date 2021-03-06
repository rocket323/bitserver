package bitserver

import (
    "os"
    "bytes"
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

// BSYNC runId fileId offset
func BSyncCmd(c *conn, args [][]byte) (redis.Resp, error) {
    if len(args) != 3 {
        return toRespErrorf("len(args) = %d, expect = 3", len(args))
    }

    s := c.s
    if (s.isSlave(c)) {
        log.Printf("conn %s is already my slave", c)
        return nil, nil
    }

    // TODO check runId

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

    // check data-files between master and slave
    if s.checkPreSync(c); err != nil {
        return nil, err
    }

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

func (s *Server) checkPreSync(c *conn) error {
    bc := s.bc
    metas := bc.GetFileMetas()

    resp, err := redis.Decode(c.r)
    if err != nil {
        return err
    }

    array, ok := resp.(*redis.Array)
    if !ok {
        return fmt.Errorf("invalid fileMetas type, expect Array, but %T", resp)
    }

    startFileId := int64(-1)
    for idx, meta := range metas {
        if idx >= len(array.Value) {
            startFileId = meta.FileId
            break
        }
        one, ok := array.Value[idx].(*redis.Array)
        if !ok {
            return err
        }
        if len(one.Value) != 2 {
            return fmt.Errorf("invalid meta len")
        }

        fileId, ok := one.Value[0].(*redis.Int)
        if !ok {
            return fmt.Errorf("invalid fileId type, expect Int, but %T", one.Value[0])
        }
        md5, ok := one.Value[1].(*redis.BulkBytes)
        if !ok {
            return fmt.Errorf("invalid md5 type, expect BulkBytes, but %T", one.Value[1])
        }

        if fileId.Value < meta.FileId {
            startFileId = fileId.Value
            break
        }
        if fileId.Value > meta.FileId {
            startFileId = meta.FileId
            break
        }

        if !bytes.Equal(meta.Md5, md5.Value) {
            startFileId = meta.FileId
            break
        }
    }

    if startFileId == -1 {
        startFileId = bc.ActiveFileId()
    }

    c.w.WriteString(fmt.Sprintf("$%d\r\n", startFileId))
    c.w.Flush()

    c.syncFileId = startFileId
    c.syncOffset = 0
    return nil
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
    ch <- struct{}{}

    s.repl.Lock()
    s.repl.slaves[c] = ch
    s.repl.Unlock()

    log.Printf("start sync to slave %s", c)
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
}

