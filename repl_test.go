package bitserver

import (
    "time"
    . "gopkg.in/check.v1"
    redis "github.com/reborndb/go/redis/resp"
)

type testReplSuite struct {
    master *testSvrNode
    slave *testSvrNode
}

var _ = Suite(&testReplSuite{})

func (s *testReplSuite) SetUpSuite(c *C) {
    port1 := 17778
    port2 := 17779
    path1 := c.MkDir()
    path2 := c.MkDir()

    s.master = &testSvrNode{port1, testCreateServer(c, port1, path1)}
    s.slave = &testSvrNode{port2, testCreateServer(c, port2, path2)}
}

func (s *testReplSuite) TearDownSuite(c *C) {
    if s.master != nil {
        s.master.Close()
    }
    if s.slave != nil {
        s.slave.Close()
    }
}

func (s *testReplSuite) TestReplication(c *C) {
    master := s.master
    slave := s.slave

    master.doCmdMustOK(c, "SLAVEOF", "NO", "ONE")
    slave.doCmdMustOK(c, "SLAVEOF", "NO", "ONE")

    master.doCmdMustOK(c, "SET", "a", "100")

    // slave sync master
    slave.doCmdMustOK(c, "SLAVEOF", "127.0.0.1", master.port)
    time.Sleep(2000 * time.Millisecond)
    resp := slave.doCmd(c, "GET", "a")
    c.Assert(resp, DeepEquals, redis.NewBulkBytesWithString("100"))

    // write to slave, must error
    resp = slave.doCmd(c, "SET", "b", "200")
    c.Assert(resp, FitsTypeOf, (*redis.Error)(nil))
    c.Assert(resp.(*redis.Error).Value, Matches, "READONLY.*")

    master.doCmdMustOK(c, "SET", "b", "100")

    time.Sleep(2000 * time.Millisecond)
    resp = slave.doCmd(c, "GET", "b")
    c.Assert(resp, DeepEquals, redis.NewBulkBytesWithString("100"))

    // check role
    master.checkRole(c, "master")
    slave.checkRole(c, "slave")

    master.doCmdMustOK(c, "SLAVEOF", "NO", "ONE")
    slave.doCmdMustOK(c, "SLAVEOF", "NO", "ONE")

    master.checkRole(c, "master")
    slave.checkRole(c, "master")
}

