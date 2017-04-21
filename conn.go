package bitserver

import (
    _ "log"
    "net"
    "time"
    "fmt"
    "bufio"
    "sync"
    redis "github.com/reborndb/go/redis/resp"
)

type conn struct {
    r *bufio.Reader
    w *bufio.Writer
    mu sync.Mutex
    s *Server
    nc net.Conn

    summ string
    timeout time.Duration
}

func newConn(nc net.Conn, s *Server, timeout int) *conn {
    c := &conn{
        nc: nc,
        s: s,
    }

    c.r = bufio.NewReader(nc)
    c.w = bufio.NewWriter(nc)
    c.summ = fmt.Sprintf("<local> %s -- %s <remote>", nc.LocalAddr(), nc.RemoteAddr())
    c.timeout = time.Duration(timeout) * time.Second
    return c
}

func (c *conn) String() string {
    return c.summ
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

    if f := c.s.htable[cmd]; f == nil {
        return toRespErrorf("unknown command %s", cmd)
    } else {
        return f.f(c.s.bc, args)
    }
}

func (c *conn) writeRESP(resp redis.Resp) error {
    if err := redis.Encode(c.w, resp); err != nil {
        return err
    }
    return c.w.Flush()
}

