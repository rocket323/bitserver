package bitserver

import (
    . "gopkg.in/check.v1"
)

type testSlotsSuite struct {
    src *testSvrNode
    dst *testSvrNode
}

var _ = Suit(&testSlotsSuite{})

func (s *testSlotsSuite) SetUpSuite(c *C) {
    port1 := 16380
    port2 := 16381
    path1 := c.MkDir()
    path2 := c.MkDir()

    s.src = &testSvrNode{port1, testCreateServer(c, port1, path1)}
    s.dst = &testSvrNode{port2, testCreateServer(c, port2, path2)}
}

func (s *testSlotsSuite) TearDownSuite(c *C) {
    if s.src != nil {
        s.src.Close()
    }
    if s.dst != nil {
        s.dst.Close()
    }
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
    dst.checkString(c, k1, "1")

    src.checkInt(c, 1, "slotsmgrtone", "127.0.0.1", dst.port, 1000, k2)
    src.checkInt(c, 0, "slotsmgrtone", "127.0.0.1", dst.port, 1000, k2)
    dst.checkString(c, k2, "2")

    src.checkOK(c, "set", k1, "3")
    src.checkInt(c, 1, "slotsmgrtone", "127.0.0.1", dst.port, 1000, k1)
    src.checkInt(c, 0, "slotsmgrtone", "127.0.0.1", dst.port, 1000, k1)
    dst.checkString(c, k1, "3")
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
    dst.checkString(c, k1, "1")

    src.checkInt(c, 0, "slotsmgrttagone", "127.0.0.1", dst.port, 1000, k2)
    dst.checkString(c, k2, "2")

    src.checkOK(c, "set", k1, "0")
    src.checkOK(c, "set", k3, "100")

    src.checkInt(c, 2, "slotsmgrttagone", "127.0.0.1", dst.port, 1000, k1)
    src.checkInt(c, 0, "slotsmgrttagone", "127.0.0.1", dst.port, 1000, k1)
    src.checkInt(c, 0, "slotsmgrttagone", "127.0.0.1", dst.port, 1000, k3)
    dst.checkString(c, k1, "0")
    dst.checkString(c, k3, "100")
}

func (s *testSlotsSuite) TestSlotsMgrtSlot(c *C) {
    src := s.src
    dst := s.dst

    k1 := "{tag}" + randomKey(c)
    k2 := "{tag}" + randomKey(c)

    src.checkOK(c, "set", k1, "1")
    rc.checkOK(c, "set", k2, "2")

    src.checkIntArray(c, []int64{1, 1}, "slotsmgrtslot", "127.0.0.1", dst.port, 1000, 899)
    src.checkIntArray(c, []int64{1, 1}, "slotsmgrtslot", "127.0.0.1", dst.port, 1000, 899)
    src.checkIntArray(c, []int64{0, 0}, "slotsmgrtslot", "127.0.0.1", dst.port, 1000, 899)

    dst.CheckString(c, 0, k1, "1")
    dst.CheckString(c, 0, k2, "2")
}

func (s *testSlotsSuite) TestSlotsMgrtTagSlot(c *C) {
    k1 := "{tag}" + randomKey(c)
    k2 := "{tag}" + randomKey(c)
    k3 := "{tag}" + randomKey(c)
    src.checkOK(c, "set", k1, "1")
    src.checkOK(c, "set", k2, "2")
    src.checkOK(c, "set", k3, "3")

    src.checkIntArray(c, []int64{3, 1}, "slotsmgrttagslot", "127.0.0.1", dst.port, 1000, 899)
    src.checkIntArray(c, []int64{0, 0}, "slotsmgrttagslot", "127.0.0.1", dst.port, 1000, 899)

    dst.checkString(c, 0, k1, "1")
    dst.checkString(c, 0, k2, "2")

    src.checkOK(c, "set", k1, "0")
    src.checkOK(c, "set", k3, "100")
    src.checkIntArray(c, []int64{2, 1}, "slotsmgrttagslot", "127.0.0.1", dst.port, 1000, 899)
    src.checkIntArray(c, []int64{0, 0}, "slotsmgrttagslot", "127.0.0.1", dst.port, 1000, 899)
    dst.checkString(c, 0, k1, "0")
    dst.checkString(c, 0, k3, "100")
}

