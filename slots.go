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
)

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
        if key, err := firstKeyUnderSlot(slot); err != nil {
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
    n, err := migrateOne(addr, timeout, key)
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

    log.Printf("migrate slot, addr = %s, timeout = %d, slot = %d", addr, timeout, slot)
    key, err := firstKeyUnderSlot(uint32(slot))
    if err != nil || key == nil {
        return toRespError(err)
    }
    n, err := migrateOne(addr, timeout, key)
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
        n, err = migrateOne(addr, timeout, key)
    } else {
        n, err = migrateTag(addr, timeout, tag)
    }

    if err != nil {
        return toRespError(err)
    }
    return redis.NewInt(n), nil
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

    key, err := firstKeyUnderSlot(uint32(slot))
    if err != nil || key == nil {
        return toRespError(err)
    }

    var n int64
    if tag := HashTag(key); len(tag) == len(key) {
        n, err = migrateOne(addr, timeout, key)
    } else {
        n, err = migrateTag(addr, timeout, tag)
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

        log.Printf("restore key = %v", key)
        if err := restore(key, expireAt, value); err != nil {
            log.Printf("restore key[%v] failed, err = %s", key, err)
            return toRespError(err)
        }
    }

    // TODO forward RESTORE to slave
    return redis.NewString("OK"), nil
}

func migrateOne(addr string, timeout time.Duration, key []byte) (int64, error) {
    n, err := migrate(addr, timeout, key)
    if err != nil {
        log.Printf("migrate one failed, err = %s", err)
        return 0, err
    }
    return n, nil
}

func migrateTag(addr string, timeout time.Duration, tag []byte) (int64, error) {
    keys, err := allKeysWithTag(tag)
    if err != nil || len(keys) == 0 {
        return 0, err
    }
    n, err := migrate(addr, timeout, keys...)
    if err != nil {
        log.Printf("migrate tag failed, err = %s", err)
        return 0, err
    }
    return n, nil
}

func migrate(addr string, timeout time.Duration, keys ...[]byte) (int64, error) {
    bc := c.s.bc

    cmd := redis.NewArray()
    cmd.AppendBulkBytes([]byte("slotsrestore"))
    for i, key := range keys {
        value, err := bc.Get(key)
        if err != nil {
            return 0, err
        }
        // TODO get ttlms
        cmd.AppendBulkBytes(key)
        cmd.AppendBulkBytes([]byte(fmt.Sprintf("%d", 0)))
        cmd.AppendBulkBytes(value)
    }

    if err := DoMustOK(cmd, timeout); err != nil {
        log.Printf("command restore failed, addr = %s, len(keys) = %d, err = %s", addr, len(keys), err)
        return 0, err
    } else {
        log.Printf("command restore ok, addr = %s, len(keys) = %d", addr, len(keys))
    }

    // delete from local
    for i, key := range keys {
        err := bc.Del(key)
        if err != nil {
            log.Printf("del key[%v] failed, err = %s", key, err)
        }
    }
    return int64(len(keys)), nil
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

