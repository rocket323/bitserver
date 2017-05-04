package bitserver

import (
    "log"
    "net"
    "time"
    "fmt"
    "bufio"
    "sync"
    "strings"
    redis "github.com/reborndb/go/redis/resp"
)

type conn struct {
    /*
        we read request from the conn goroutine,
        but may write resp from other goroutine,
        so we only need write lock.
    */
    wLock sync.Mutex

    r *bufio.Reader
    w *bufio.Writer
    s *Server
    nc net.Conn

    summ string
    timeout time.Duration

    // replication
    syncFileId int64
    syncOffset int64
    isSyncing bool
}

func newConn(nc net.Conn, s *Server, timeout int) *conn {
    c := &conn{
        nc: nc,
        s: s,
        r: bufio.NewReader(nc),
        w: bufio.NewWriter(nc),
        summ: fmt.Sprintf("<local> %s -- %s <remote>", nc.LocalAddr(), nc.RemoteAddr()),
        timeout: time.Duration(timeout) * time.Second,
    }
    return c
}

func (c *conn) String() string {
    return c.summ
}

func (c *conn) Close() {
    c.nc.Close()
    c = nil
}

func (c *conn) serve() error {
    defer func() {
        c.s.removeConn(c)
    }()
    c.s.addConn(c)

    for {
        response, err := c.handleRequest()
        if err != nil {
            return err
        } else if response == nil {
            continue
        }

        if c.timeout > 0 {
            deadline := time.Now().Add(c.timeout)
            if err := c.nc.SetWriteDeadline(deadline); err != nil {
                return err
            }
        }
        if err = c.writeRESP(response); err != nil {
            return err
        }
    }
    return nil
}

func (c *conn) handleRequest() (redis.Resp, error) {
    if c.timeout > 0 {
        deadline := time.Now().Add(c.timeout)
        if err := c.nc.SetReadDeadline(deadline); err != nil {
            return nil, err
        }
    }
    request, err := redis.DecodeRequest(c.r)
    if err != nil {
        return nil, err
    }

    if request.Type() == redis.TypePing {
        return nil, nil
    }

    response, err := c.dispatch(request)
    if err != nil {
        return response, nil
    }

    return response, nil
}

func (c *conn) dispatch(request redis.Resp) (redis.Resp, error) {
    cmd, args, err := redis.ParseArgs(request)
    if err != nil {
        return toRespError(err)
    }
    s := c.s

    if f := c.s.htable[cmd]; f == nil {
        log.Printf("unknown command %s", cmd)
        return toRespErrorf("unknown command %s", cmd)
    } else {
        masterAddr := s.repl.masterAddr.Get()
        if len(masterAddr) > 0 && f.flag&CmdWrite > 0 {
            return toRespErrorf("READONLY You can't write against a read only slave.")
        }

        return f.f(c, args)
    }
}

func (c *conn) writeRESP(resp redis.Resp) error {
    c.wLock.Lock()
    defer c.wLock.Unlock()

    if err := redis.Encode(c.w, resp); err != nil {
        return err
    }
    return c.w.Flush()
}

func (c *conn) ping() error {
    if err := c.writeRESP(redis.NewRequest("PING")); err != nil {
        return err
    }
    if rsp, err := c.readLine(); err != nil {
        return err
    } else if strings.ToLower(string(rsp)) != "+pong" {
        return fmt.Errorf("invalid response of command ping: %s", rsp)
    }
    return nil
}

func (c *conn) readLine() (line []byte, err error) {
    // if we read too many \n only, maybe something is wrong.
    for i := 0; i < 100; i++ {
        line, err = c.r.ReadSlice('\n')
        if err != nil {
            return nil, err
        } else if line[0] == '\n' {
            // only \n one line, try again
            continue
        }
        break
    }

    i := len(line) - 2
    if i < 0 || line[i] != '\r' {
        return nil, fmt.Errorf("bad resp line terminator")
    }
    return line[:i], nil
}

