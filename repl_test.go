package bitserver

import (
    "time"
    . "gopkg.in/check.v1"
    redis "github.com/reborndb/go/redis/resp"
)

type testReplNode struct {
    port int
    svr *Server
}

func (nd *testReplNode) Close() {
    nd.svr.Close()
}

type testReplSuite struct {
    master *testReplNode
    slave *testReplNode
}

var _ = Suite(&testReplSuite{})

func (s *testReplSuite) SetUpSuite(c *C) {
    port1 := 17778
    port2 := 17779
    path1 := c.MkDir()
    path2 := c.MkDir()

    s.master = &testReplNode{port1, testCreateServer(c, port1, path1)}
    s.slave = &testReplNode{port2, testCreateServer(c, port2, path2)}
}

func (s *testReplSuite) TearDownSuite(c *C) {
    if s.master != nil {
        s.master.Close()
    }
    if s.slave != nil {
        s.slave.Close()
    }
}

func (s *testReplSuite) doCmd(c *C, port int, cmd string, args ...interface{}) redis.Resp {
    nc := testGetConn(c, port)
    defer nc.Close()
    return nc.doCmd(c, cmd, args...)
}

func (s *testReplSuite) doCmdMustOK(c *C, port int, cmd string, args ...interface{}) {
    nc := testGetConn(c, port)
    defer nc.Close()
    nc.checkOK(c, cmd, args...)
}

func (s *testReplSuite) checkRole(c *C, port int, expect string) {
    r := s.doCmd(c, port, "ROLE")
    resp, ok := r.(*redis.Array)
    c.Assert(ok, Equals, true)
    c.Assert(resp.Value, Not(HasLen), 0)
    role, ok := resp.Value[0].(*redis.BulkBytes)
    c.Assert(ok, Equals, true)
    c.Assert(string(role.Value), Equals, expect)
}

func (s *testReplSuite) TestReplication(c *C) {
    master := s.master
    slave := s.slave

    s.doCmdMustOK(c, master.port, "SLAVEOF", "NO", "ONE")
    s.doCmdMustOK(c, slave.port, "SLAVEOF", "NO", "ONE")

    s.doCmdMustOK(c, master.port, "SET", "a", "100")

    // slave sync master
    s.doCmdMustOK(c, slave.port, "SLAVEOF", "127.0.0.1", master.port)
    resp := s.doCmd(c, slave.port, "GET", "a")
    c.Assert(resp, DeepEquals, redis.NewBulkBytesWithString("100"))

    // write to slave, must error
    resp = s.doCmd(c, slave.port, "SET", "b", "200")
    c.Assert(resp, FitsTypeOf, (*redis.Error)(nil))
    c.Assert(resp.(*redis.Error).Value, Matches, "READONLY.*")

    s.doCmdMustOK(c, master.port, "SET", "b", "100")

    time.Sleep(500 * time.Millisecond)
    resp = s.doCmd(c, slave.port, "GET", "b")
    c.Assert(resp, DeepEquals, redis.NewBulkBytesWithString("100"))

    // check role
    s.checkRole(c, master.port, "master")
    s.checkRole(c, slave.port, "slave")

    s.doCmdMustOK(c, master.port, "SLAVEOF", "NO", "ONE")
    s.doCmdMustOK(c, slave.port, "SLAVEOF", "NO", "ONE")

    s.checkRole(c, master.port, "master")
    s.checkRole(c, slave.port, "master")
}

