package bitserver

import (
    "container/list"
    "net"
    "bufio"
    "time"
    "log"
    "fmt"
    "sync"

    redis "github.com/reborndb/go/redis/resp"
    "github.com/rocket323/bitcask"
)

var mgrtPoolMap struct {
    m map[string]*list.List
    sync.Mutex
}

type mgrtConn struct {
    summ string
    nc net.Conn
    last time.Time
    err error
    r *bufio.Reader
    w *bufio.Writer
}

func (c *mgrtConn) encodeResp(resp redis.Resp, timeout time.Duration) error {
    if err := c.nc.SetWriteDeadline(time.Now().Add(timeout)); err != nil {
        return err
    }
    if err := redis.Encode(c.w, resp); err != nil {
        return err
    }
    return c.w.Flush()
}

func (c *mgrtConn) decodeResp(timeout time.Duration) (redis.Resp, error) {
    if err := c.nc.SetReadDeadline(time.Now().Add(timeout)); err != nil {
        return nil, err
    }
    return redis.Decode(c.r)
}

func (c *mgrtConn) Do(cmd *redis.Array, timeout time.Duration) (redis.Resp, error) {
    if c.err != nil {
        return nil, c.err
    }
    if err := c.encodeResp(cmd, timeout); err != nil {
        c.err = err
        log.Printf("encode resp failed, err = %s", err)
        return nil, err
    }
    if rsp, err := c.decodeResp(timeout); err != nil {
        c.err = err
        log.Printf("decode resp failed, err = %s", err)
        return nil, err
    } else {
        c.last = time.Now()
        return rsp, nil
    }
}

func (c *mgrtConn) DoMustOK(cmd *redis.Array, timeout time.Duration) error {
    if rsp, err := c.Do(cmd, timeout); err != nil {
        return err
    } else {
        s, ok := rsp.(*redis.String)
        if ok {
            if s.Value == "OK" {
                return nil
            }
            c.err = fmt.Errorf("not ok, got %s", s.Value)
        } else {
            c.err = fmt.Errorf("not string response, got %v", rsp.Type())
        }
        return c.err
    }
}

func (c *mgrtConn) String() string {
    return c.summ
}

const maxConnIdletime = 10 * time.Second

func init() {
    mgrtPoolMap.m = make(map[string]*list.List)
    go func () {
        time.Sleep(time.Second)
        mgrtPoolMap.Lock()
        for addr, pool := range mgrtPoolMap.m {
            for i := pool.Len(); i != 0; i-- {
                c := pool.Remove(pool.Front()).(*mgrtConn)
                if time.Now().Before(c.last.Add(maxConnIdletime)) {
                    pool.PushBack(c)
                } else {
                    c.nc.Close()
                    log.Printf("close mgrt connection %s : %s", addr, c)
                }
            }
            if pool.Len() != 0 {
                continue
            }
            delete(mgrtPoolMap.m, addr)
        }
        mgrtPoolMap.Unlock()
    }()
}

func getMgrtConn(addr string, timeout time.Duration) (*mgrtConn, error) {
    mgrtPoolMap.Lock()
    if pool := mgrtPoolMap.m[addr]; pool != nil && pool.Len() != 0 {
        c := pool.Remove(pool.Front()).(*mgrtConn)
        mgrtPoolMap.Unlock()
        return c, nil
    }
    mgrtPoolMap.Unlock()
    nc, err := net.DialTimeout("tcp", addr, timeout)
    if err != nil {
        return nil, err
    }

    c := &mgrtConn{
        summ: fmt.Sprintf("<local> %s -- %s <remote>", nc.LocalAddr(), nc.RemoteAddr()),
        nc: nc,
        last: time.Now(),
        r: bufio.NewReader(nc),
        w: bufio.NewWriter(nc),
    }
    log.Printf("create mgrt connection %s: %s", addr, c)
    return c, nil
}

func putMgrtConn(addr string, c *mgrtConn) {
    if c.err != nil {
        c.nc.Close()
        log.Printf("close err mgrt connection %s: %s, err = %s", addr, c, c.err)
    } else {
        mgrtPoolMap.Lock()
        pool := mgrtPoolMap.m[addr]
        if pool == nil {
            pool = list.New()
            mgrtPoolMap.m[addr] = pool
        }
        c.last = time.Now()
        pool.PushFront(c)
        mgrtPoolMap.Unlock()
    }
}

func doMigrate(bc *bitcask.BitCask, addr string, timeout time.Duration, keys ...[]byte) (int64, error) {
    c, err := getMgrtConn(addr, timeout)
    if err != nil {
        log.Printf("connect to %s failed, timeout = %d, err = %s", addr, timeout, err)
        return 0, err
    }
    defer putMgrtConn(addr, c)

    cmd := redis.NewArray()
    cmd.AppendBulkBytes([]byte("slotsrestore"))
    var cnt int64 = 0
    for _, key := range keys {
        value, err := bc.Get(string(key))
        if err != nil {
            log.Printf("mgrt key[%s] missing", key)
            continue
        }
        // TODO get ttlms
        cmd.AppendBulkBytes(key)
        cmd.AppendBulkBytes([]byte(fmt.Sprintf("%d", 0)))
        cmd.AppendBulkBytes(value)
        cnt++
    }

    if cnt  == 0 {
        log.Printf("no key to migrate")
        return 0, nil
    }

    if err := c.DoMustOK(cmd, timeout); err != nil {
        log.Printf("command restore failed, addr = %s, len(keys) = %d, err = %s", addr, len(keys), err)
        return 0, err
    } else {
        log.Printf("command restore ok, addr = %s, cnt = %d", addr, cnt)
        return cnt, nil
    }
}

