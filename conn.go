package bitserver

import (
    "net"
    "time"
    "fmt"
    redis "github.com/reborndb/go/redis/resp"
)

type conn struct {
    r *bufio.Reader,
    w *bufio.Writer,
    mu sync.Mutex,
    s *Server,

    summ string,
    timeout time.Duration,
}

func newConn(nc, net.Conn, s *Server, timeout int) *conn {
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
        c.h.removeConn(c)
    }()
    c.h.addConn(c)

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
        return nil, err
    }

    return response, nil
}

func (c *conn) dispatch() (redis.Resp, error) {
    cmd, args, err := redis.ParseArgs(request)
    if err != nil {
        return toRespError(err)
    }
}

