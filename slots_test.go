package bitserver

import (
    "log"
    . "gopkg.in/check.v1"
)

type testSlotsSuite struct {
    src *testSvrNode
    dst *testSvrNode
}

var _ = Suite(&testSlotsSuite{})

var curPort = 16380

func (s *testSlotsSuite) SetUpTest(c *C) {
    port1 := curPort
    port2 := curPort + 1
    path1 := c.MkDir()
    path2 := c.MkDir()
    curPort += 2

    s.src = testCreateServer(c, port1, path1)
    s.dst = testCreateServer(c, port2, path2)
}

func (s *testSlotsSuite) TearDownTest(c *C) {
    log.Printf("tear down!!!!")
    if s.src != nil {
        s.src.Close()
    }
    if s.dst != nil {
        s.dst.Close()
    }
}

func (s *testSlotsSuite) TestSlotsHashKey(c *C) {
    s.src.checkIntArray(c, []int64{579, 1017, 879}, "slotshashkey", "a", "b", "c")
}

func (s *testSlotsSuite) TestSlotsMgrtSlot(c *C) {
    src := s.src
    dst := s.dst

    k1 := "{tag}" + randomKey(c)
    k2 := "{tag}" + randomKey(c)

    src.checkOK(c, "set", k1, "1")
    src.checkOK(c, "set", k2, "2")

    src.checkIntArray(c, []int64{1, 1}, "slotsmgrtslot", "127.0.0.1", dst.port, 1000, 899)
    src.checkIntArray(c, []int64{1, 1}, "slotsmgrtslot", "127.0.0.1", dst.port, 1000, 899)
    src.checkIntArray(c, []int64{0, 0}, "slotsmgrtslot", "127.0.0.1", dst.port, 1000, 899)

    dst.checkString(c, "1", "get", k1)
    dst.checkString(c, "2", "get", k2)
}

func (s *testSlotsSuite) TestSlotsMgrtTagSlot(c *C) {
    src := s.src
    dst := s.dst

    k1 := "{tag}" + randomKey(c)
    k2 := "{tag}" + randomKey(c)
    k3 := "{tag}" + randomKey(c)
    src.checkOK(c, "set", k1, "1")
    src.checkOK(c, "set", k2, "2")
    src.checkOK(c, "set", k3, "3")

    src.checkIntArray(c, []int64{3, 1}, "slotsmgrttagslot", "127.0.0.1", dst.port, 1000, 899)
    src.checkIntArray(c, []int64{0, 0}, "slotsmgrttagslot", "127.0.0.1", dst.port, 1000, 899)

    dst.checkString(c, "1", "get", k1)
    dst.checkString(c, "2", "get", k2)

    src.checkOK(c, "set", k1, "0")
    src.checkOK(c, "set", k3, "100")
    src.checkIntArray(c, []int64{2, 1}, "slotsmgrttagslot", "127.0.0.1", dst.port, 1000, 899)
    src.checkIntArray(c, []int64{0, 0}, "slotsmgrttagslot", "127.0.0.1", dst.port, 1000, 899)
    dst.checkString(c, "0", "get", k1)
    dst.checkString(c, "100", "get", k3)
}

func (s *testSlotsSuite) TestSlotsMgrtOne(c *C) {
    src := s.src
    dst := s.dst

    k1 := "{tag}" + randomKey(c)
    k2 := "{tag}" + randomKey(c)
    src.checkOK(c, "set", k1, "1")
    src.checkOK(c, "set", k2, "2")
    src.checkInt(c, 1, "slotsmgrtone", "127.0.0.1", dst.port, 1000, k1)
    src.checkInt(c, 0, "slotsmgrtone", "127.0.0.1", dst.port, 1000, k1)
    dst.checkString(c, "1", "get", k1)

    src.checkInt(c, 1, "slotsmgrtone", "127.0.0.1", dst.port, 1000, k2)
    src.checkInt(c, 0, "slotsmgrtone", "127.0.0.1", dst.port, 1000, k2)
    dst.checkString(c, "2", "get", k2)

    src.checkOK(c, "set", k1, "3")
    src.checkInt(c, 1, "slotsmgrtone", "127.0.0.1", dst.port, 1000, k1)
    src.checkInt(c, 0, "slotsmgrtone", "127.0.0.1", dst.port, 1000, k1)
    dst.checkString(c, "3", "get", k1)
}

func (s *testSlotsSuite) TestSlotsMgrtTagOne(c *C) {
    src := s.src
    dst := s.dst

    k1 := "{tag}" + randomKey(c)
    k2 := "{tag}" + randomKey(c)
    k3 := "{tag}" + randomKey(c)

    src.checkOK(c, "set", k1, "1")
    src.checkOK(c, "set", k2, "2")

    src.checkInt(c, 2, "slotsmgrttagone", "127.0.0.1", dst.port, 1000, k1)
    src.checkInt(c, 0, "slotsmgrttagone", "127.0.0.1", dst.port, 1000, k1)
    dst.checkString(c, "1", "get", k1)

    src.checkInt(c, 0, "slotsmgrtone", "127.0.0.1", dst.port, 1000, k2)
    dst.checkString(c, "2", "get", k2)

    src.checkOK(c, "set", k1, "0")
    src.checkOK(c, "set", k3, "100")

    src.checkInt(c, 2, "slotsmgrttagone", "127.0.0.1", dst.port, 1000, k1)
    src.checkInt(c, 0, "slotsmgrtone", "127.0.0.1", dst.port, 1000, k1)
    src.checkInt(c, 0, "slotsmgrtone", "127.0.0.1", dst.port, 1000, k3)
    dst.checkString(c, "0", "get", k1)
    dst.checkString(c, "100", "get", k3)
}

