package bitserver

import (
    "time"
    "bufio"
    "fmt"
    "net"
    "math/rand"
    "log"
    "testing"
    . "gopkg.in/check.v1"
    redis "github.com/reborndb/go/redis/resp"
)

func Test(t *testing.T) { TestingT(t) }

// var _ = Suite(&serverSuite{})

type testSvrNode struct {
    port int
    path string
    svr *Server
}

func (nd *testSvrNode) Close() {
    nd.svr.Close()
}

func (s *testSvrNode) doCmd(c *C, cmd string, args ...interface{}) redis.Resp {
    nc := testGetConn(c, s.port)
    defer nc.Close()
    return nc.doCmd(c, cmd, args...)
}

func (s *testSvrNode) checkOK(c *C, cmd string, args ...interface{}) {
    nc := testGetConn(c, s.port)
    defer nc.Close()
    nc.checkOK(c, cmd, args...)
}

func (s *testSvrNode) checkInt(c *C, expect int64, cmd string, args ...interface{}) {
    nc := testGetConn(c, s.port)
    defer nc.Close()
    nc.checkInt(c, expect, cmd, args...)
}

func (s *testSvrNode) checkString(c *C, expect string, cmd string, args ...interface{}) {
    nc := testGetConn(c, s.port)
    defer nc.Close()
    nc.checkString(c, expect, cmd, args...)
}

func (s *testSvrNode) checkIntArray(c *C, expect []int64, cmd string, args ...interface{}) {
    nc := testGetConn(c, s.port)
    defer nc.Close()
    nc.checkIntArray(c, expect, cmd, args...)
}

func (s *testSvrNode) checkRole(c *C, expect string) {
    r := s.doCmd(c, "ROLE")
    resp, ok := r.(*redis.Array)
    c.Assert(ok, Equals, true)
    c.Assert(resp.Value, Not(HasLen), 0)
    role, ok := resp.Value[0].(*redis.BulkBytes)
    c.Assert(ok, Equals, true)
    c.Assert(string(role.Value), Equals, expect)
}

func testCreateServer(c *C, port int, dbpath string) *testSvrNode {
    config := DefaultConfig()
    config.Dbpath = dbpath
    config.Listen = port
    s, err := NewServer(config)
    c.Assert(err, IsNil)

    go func() {
        s.Serve()
    }()

    node := &testSvrNode{
        port: port,
        path: dbpath,
        svr: s,
    }

    return node
}

type serverSuite struct {
    s *testSvrNode
}

func (s *serverSuite) SetUpSuite(c *C) {
    port := 12345
    path := c.MkDir()
    log.Printf("store path: %s", path)

    s.s = testCreateServer(c, port, path)
}

func (s *serverSuite) TearDownSuite(c *C) {
    s.s.Close()
}

var (
    keySet = make(map[string]bool)
)

func randomKey(c *C) string {
    for i := 0; ; i++ {
        p := make([]byte, 16)
        for j := 0; j < len(p); j++ {
            p[j] = 'a' + byte(rand.Intn(26))
        }
        s := "key_" + string(p)
        if _, ok := keySet[s]; !ok {
            keySet[s] = true
            return s
        }
        c.Assert(i < 32, Equals, true)
    }
}

type testConn struct {
    nc net.Conn
}

func testGetConn(c *C, port int) *testConn {
    url := fmt.Sprintf("127.0.0.1:%d", port)
    nc, err := net.Dial("tcp", url)
    if err != nil {
        log.Printf("dial server[%s] failed, err = %s", url, err)
    }
    c.Assert(err, IsNil)

    return &testConn{
        nc: nc,
    }
}

func (tc *testConn) Close() {
    tc.nc.Close()
}

func (tc *testConn) doCmd(c *C, cmd string, args ...interface{}) redis.Resp {
    r := bufio.NewReaderSize(tc.nc, 32)
    w := bufio.NewWriterSize(tc.nc, 32)

    req := redis.NewRequest(cmd, args...)
    err := redis.Encode(w, req)
    c.Assert(err, IsNil)

    err = w.Flush()
    c.Assert(err, IsNil)

    resp, err := redis.Decode(r)
    c.Assert(err, IsNil)

    return resp
}

func (tc *testConn) checkOK(c *C, cmd string, args ...interface{}) {
    tc.checkString(c, "OK", cmd, args...)
}

func (tc *testConn) checkString(c *C, expect string, cmd string, args ...interface{}) {
    resp := tc.doCmd(c, cmd, args...)
    switch x := resp.(type) {
    case *redis.String:
        c.Assert(x.Value, Equals, expect)
    case *redis.BulkBytes:
        c.Assert(string(x.Value), Equals, expect)
    default:
        c.Errorf("invalid type %T", resp)
    }
}

func (tc *testConn) checkInt(c *C, expect int64, cmd string, args ...interface{}) {
    resp := tc.doCmd(c, cmd, args...)
    c.Assert(resp, DeepEquals, redis.NewInt(expect))
}

func (tc *testConn) checkIntArray(c *C, expect []int64, cmd string, args ...interface{}) {
    resp := tc.doCmd(c, cmd, args...)
    v, ok := resp.(*redis.Array)
    c.Assert(ok, Equals, true)
    c.Assert(v.Value, HasLen, len(expect))

    for i, vv := range v.Value {
        b, ok := vv.(*redis.Int)
        c.Assert(ok, Equals, true)
        c.Assert(b.Value, Equals, expect[i])
    }
}

func init() {
    rand.Seed(time.Now().UnixNano())
    log.SetFlags(log.Lshortfile | log.LstdFlags)
}

func (s *serverSuite) TestServer(c *C) {
    svr := s.s

    k1 := randomKey(c)
    svr.checkOK(c, "set", k1, "hello")
    k2 := randomKey(c)
    svr.checkOK(c, "set", k2, "world")

    svr.checkString(c, "hello", "get", k1)
    svr.checkString(c, "world", "get", k2)
}

