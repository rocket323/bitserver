package bitserver

import (
    "fmt"
    "log"
    "strconv"
    "hash/crc32"
    "time"
    "bytes"
    redis "github.com/reborndb/go/redis/resp"
)

const (
    MaxSlotNum = 1024
    MaxExpireAt = 1e15
)

func TTLmsToExpireAt(ttlms int64) (int64, bool) {
    if ttlms < 0 || ttlms > MaxExpireAt {
        return 0, false
    }
    nowms := int64(time.Now().UnixNano()) / int64(time.Millisecond)
    expireat := nowms + ttlms
    if expireat > MaxExpireAt {
        return 0, false
    }
    return expireat, true
}

func HashTag(key []byte) []byte {
    part := key
    if i := bytes.IndexByte(part, '{'); i != -1 {
        part = part[i+1:]
    } else {
        return key
    }
    if i := bytes.IndexByte(part, '}'); i != -1 {
        return part[:i]
    } else {
        return key
    }
}

func HashTagToSlot(tag []byte) uint32 {
    return crc32.ChecksumIEEE(tag) % MaxSlotNum
}

func HashKeyToSlot(key []byte) ([]byte, uint32) {
    tag := HashTag(key)
    return tag, HashTagToSlot(tag)
}

// SLOTSHASHKEY key [key...]
func SlotsHashKeyCmd(c *conn, args [][]byte) (redis.Resp, error) {
    resp := redis.NewArray()
    for _, key := range args {
        _, slot := HashKeyToSlot(key)
        resp.AppendInt(int64(slot))
    }
    return resp, nil
}

// SLOTSINFO [start [count]]
func SlotsInfoCmd(c *conn, args [][]byte) (redis.Resp, error) {
    if len(args) > 2 {
        return toRespErrorf("len(args) = %d, expect <= 2", len(args))
    }

    var start, count int64 = 0, MaxSlotNum
    var err error
    if len(args) >= 1 {
        start, err = strconv.ParseInt(string(args[0]), 10, 64)
        if err != nil {
            return toRespError(err)
        }
    }
    if len(args) >= 2 {
        count, err = strconv.ParseInt(string(args[1]), 10, 64)
        if err != nil {
            return toRespError(err)
        }
    }
    limit := start + count
    bc := c.s.bc

    resp := redis.NewArray()
    for slot := uint32(start); slot < uint32(limit) && slot < MaxSlotNum; slot++ {
        if key, err := bc.FirstKeyUnderSlot(slot); err != nil {
            return toRespError(err)
        } else {
            var cnt int
            if key != nil {
                cnt = 1
            } else {
                cnt = 0
            }
            s := redis.NewArray()
            s.AppendInt(int64(slot))
            s.AppendInt(int64(cnt))
            resp.Append(s)
        }
    }
    return resp, nil
}

// SLOTSMGRTONE host port timeout key
func SlotsMgrtOneCmd(c *conn, args [][]byte) (redis.Resp, error) {
    if len(args) != 4 {
        return toRespErrorf("len(args) = %d, expect = 4", len(args))
    }
    host := string(args[0])
    port, err := strconv.ParseInt(string(args[1]), 10, 64)
    if err != nil {
        return toRespError(err)
    }
    ttlms, err := strconv.ParseInt(string(args[2]), 10, 64)
    if err != nil {
        return toRespError(err)
    }
    key := args[3]

    var timeout = time.Duration(ttlms) * time.Millisecond
    if timeout == 0 {
        timeout = time.Second
    }
    addr := fmt.Sprintf("%s:%d", host, port)

    log.Printf("migrate one, addr = %s, timeout = %d, key = %v", addr, timeout, key)
    n, err := migrateOne(c, addr, timeout, key)
    if err != nil {
        return toRespError(err)
    }
    return redis.NewInt(n), nil
}

// SLOTSMGRTTAGONE host port timeout key
func SlotsMgrtTagOneCmd(c *conn, args [][]byte) (redis.Resp, error) {
    if len(args) != 4 {
        return toRespErrorf("len(args) = %d, expect = 4", len(args))
    }
    host := string(args[0])
    port, err := strconv.ParseInt(string(args[1]), 10, 64)
    if err != nil {
        return toRespError(err)
    }
    ttlms, err := strconv.ParseInt(string(args[2]), 10, 64)
    if err != nil {
        return toRespError(err)
    }
    key := args[3]

    var timeout = time.Duration(ttlms) * time.Millisecond
    if timeout == 0 {
        timeout = time.Second
    }
    addr := fmt.Sprintf("%s:%d", host, port)

    log.Printf("migrate one with tag, addr = %s, timeout = %d, key = %v", addr, timeout, key)
    var n int64
    if tag := HashTag(key); len(tag) == len(key) {
        n, err = migrateOne(c, addr, timeout, key)
    } else {
        n, err = migrateTag(c, addr, timeout, tag)
    }

    if err != nil {
        return toRespError(err)
    }
    return redis.NewInt(n), nil
}

// SLOTSMGRTSLOT host port timeout slot
func SlotsMgrtSlotCmd(c *conn, args [][]byte) (redis.Resp, error) {
    if len(args) != 4 {
        return toRespErrorf("len(args) = %d, expect = 4", len(args))
    }
    host := string(args[0])
    port, err := strconv.ParseInt(string(args[1]), 10, 64)
    if err != nil {
        return toRespError(err)
    }
    ttlms, err := strconv.ParseInt(string(args[2]), 10, 64)
    if err != nil {
        return toRespError(err)
    }
    slot, err := strconv.ParseInt(string(args[3]), 10, 64)
    if err != nil {
        return toRespError(err)
    }

    var timeout = time.Duration(ttlms) * time.Millisecond
    if timeout == 0 {
        timeout = time.Second
    }
    addr := fmt.Sprintf("%s:%d", host, port)

    bc := c.s.bc
    // log.Printf("migrate slot, addr = %s, timeout = %d, slot = %d", addr, timeout, slot)
    key, err := bc.FirstKeyUnderSlot(uint32(slot))
    if err != nil {
        return toRespError(err)
    }
    var n int64 = 0
    if key != nil {
        n, err = migrateOne(c, addr, timeout, key)
        if err != nil {
            return toRespError(err)
        }
    }

    resp := redis.NewArray()
    resp.AppendInt(n)
    if n != 0 {
        resp.AppendInt(1)
    } else {
        resp.AppendInt(0)
    }
    return resp, nil
}

// SLOTSMGRTTAGSLOT host port timeout slot
func SlotsMgrtTagSlotCmd(c *conn, args [][]byte) (redis.Resp, error) {
    if len(args) != 4 {
        return toRespErrorf("len(args) = %d, expect = 4", len(args))
    }
    host := string(args[0])
    port, err := strconv.ParseInt(string(args[1]), 10, 64)
    if err != nil {
        return toRespError(err)
    }
    ttlms, err := strconv.ParseInt(string(args[2]), 10, 64)
    if err != nil {
        return toRespError(err)
    }
    slot, err := strconv.ParseInt(string(args[3]), 10, 64)
    if err != nil {
        return toRespError(err)
    }

    var timeout = time.Duration(ttlms) * time.Millisecond
    if timeout == 0 {
        timeout = time.Second
    }
    addr := fmt.Sprintf("%s:%d", host, port)

    bc := c.s.bc
    key, err := bc.FirstKeyUnderSlot(uint32(slot))
    if err != nil || key == nil {
        return toRespError(err)
    }

    var n int64
    if tag := HashTag(key); len(tag) == len(key) {
        n, err = migrateOne(c, addr, timeout, key)
    } else {
        n, err = migrateTag(c, addr, timeout, tag)
    }
    if err != nil {
        return toRespError(err)
    }

    resp := redis.NewArray()
    resp.AppendInt(n)
    if n != 0 {
        resp.AppendInt(1)
    } else {
        resp.AppendInt(0)
    }
    return resp, nil
}

// SLOTSRESTORE key ttlms value [key ttlms value...]
func SlotsRestoreCmd(c *conn, args [][]byte) (redis.Resp, error) {
    if len(args) == 0 || len(args) % 3 != 0 {
        return toRespErrorf("len(args) = %d, expect != 0 && mod 3 = 0", len(args))
    }

    bc := c.s.bc
    num := len(args) / 3
    for i := 0; i < num; i++ {
        key := args[i * 3]
        ttlms, err := strconv.ParseInt(string(args[i * 3 + 1]), 10, 64)
        if err != nil {
            return toRespError(err)
        }
        value := args[i * 3 + 2]

        expireAt := int64(0)
        if ttlms != 0 {
            if v, ok := TTLmsToExpireAt(ttlms); ok && v > 0 {
                expireAt = v
            } else {
                return toRespErrorf("parse args[%d] ttls = %d", i*3+1, ttlms)
            }
        }

        // log.Printf("restore key = %v", key)
        if err := bc.SetWithExpr(key, value, uint32(expireAt)); err != nil {
            log.Printf("restore key[%v] failed, err = %s", key, err)
            return toRespError(err)
        }
    }

    return redis.NewString("OK"), nil
}

func migrateOne(c *conn, addr string, timeout time.Duration, key []byte) (int64, error) {
    n, err := migrate(c, addr, timeout, key)
    if err != nil {
        log.Printf("migrate one failed, err = %s", err)
        return 0, err
    }
    return n, nil
}

func migrateTag(c *conn, addr string, timeout time.Duration, tag []byte) (int64, error) {
    bc := c.s.bc
    keys, err := bc.AllKeysWithTag(tag)
    if err != nil || len(keys) == 0 {
        return 0, err
    }
    n, err := migrate(c, addr, timeout, keys...)
    if err != nil {
        log.Printf("migrate tag failed, err = %s", err)
        return 0, err
    }
    return n, nil
}

func migrate(c *conn, addr string, timeout time.Duration, keys ...[]byte) (int64, error) {
    bc := c.s.bc

    cnt, err := doMigrate(bc, addr, timeout, keys...);
    if err != nil {
        log.Printf("migrate failed, err = %s", err)
        return 0, err
    }

    // delete from local
    for _, key := range keys {
        err := bc.DelLocal(key)
        if err != nil {
            log.Printf("del key[%v] failed, err = %s", key, err)
        }
    }
    return cnt, nil
}

func init() {
    Register("slotshashkey", SlotsHashKeyCmd, CmdReadOnly)
    Register("slotsinfo", SlotsInfoCmd, CmdReadOnly)
    Register("slotsmgrtone", SlotsMgrtOneCmd, CmdWrite)
    Register("slotsmgrtslot", SlotsMgrtSlotCmd, CmdWrite)
    Register("slotsmgrttagone", SlotsMgrtTagOneCmd, CmdWrite)
    Register("slotsmgrttagslot", SlotsMgrtTagSlotCmd, CmdWrite)
    Register("slotsrestore", SlotsRestoreCmd, CmdWrite)
}

