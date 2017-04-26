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

type serverSuite struct {
    s *Server
    path string
    port int
}

var _ = Suite(&serverSuite{})

func testCreateServer(c *C, port int, dbpath string) *Server {
    config := DefaultConfig()
    config.Dbpath = dbpath
    config.Listen = port
    s, err := NewServer(config)
    c.Assert(err, IsNil)

    go func() {
        s.Serve()
    }()

    return s
}

func (s *serverSuite) SetUpSuite(c *C) {
    s.port = 12345
    s.path = c.MkDir()
    log.Printf("store path: %s", s.path)

    s.s = testCreateServer(c, s.port, s.path)
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

func (s *serverSuite) checkOK(c *C, cmd string, args ...interface{}) {
    tc := testGetConn(c, s.port)
    tc.checkOK(c, cmd, args...)
}

func (s *serverSuite) checkString(c *C, expect string, cmd string, args ...interface{}) {
    tc := testGetConn(c, s.port)
    tc.checkString(c, expect, cmd, args...)
}

func init() {
    rand.Seed(time.Now().UnixNano())
}

func (s *serverSuite) TestServer(c *C) {
    k1 := randomKey(c)
    s.checkOK(c, "set", k1, "hello")
    k2 := randomKey(c)
    s.checkOK(c, "set", k2, "world")

    s.checkString(c, "hello", "get", k1)
    s.checkString(c, "world", "get", k2)
}

