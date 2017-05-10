package bitserver

import (
    "strconv"
    "strings"
    "log"
    "github.com/rocket323/bitcask"
    redis "github.com/reborndb/go/redis/resp"
)

type command struct {
    name    string
    f       CommandFunc
    flag    CommandFlag
}

var globalCommand = make(map[string]*command)

func register(name string, f CommandFunc, flag CommandFlag) {
    funcName := strings.ToLower(name)
    if _, ok := globalCommand[funcName]; ok {
        log.Fatalf("%s has been registered", name)
    }
    globalCommand[funcName] = &command{name, f, flag}
}

type CommandFunc func(c *conn, args [][]byte) (redis.Resp, error)
type CommandFlag uint32

const (
    CmdWrite CommandFlag = 1 << iota
    CmdReadOnly
)

func Register(name string, f CommandFunc, flag CommandFlag) {
    register(name, f, flag)
}

// GET key
func GetCmd(c *conn, args [][]byte) (redis.Resp, error) {
    if len(args) < 1 {
        return toRespErrorf("len(args) = %d, expect >= 1", len(args))
    }

    key := args[0]
    bc := c.s.bc

    value, err := bc.Get(key)
    if err != nil && err != bitcask.ErrKeyNotFound {
        return toRespError(err)
    } else {
        return redis.NewBulkBytes(value), nil
    }
}

// SET key value [EX seconds]
func SetCmd(c *conn, args [][]byte) (redis.Resp, error) {
    if len(args) < 2 {
        return toRespErrorf("len(args) = %d, expect >= 2", len(args))
    }

    key := args[0]
    value := args[1]
    bc := c.s.bc

    err := bc.Set(key, value)
    if err != nil {
        return toRespError(err)
    } else {
        return redis.NewString("OK"), nil
    }
}

// DEL KEY [KEY ...]
func DelCmd(c *conn, args [][]byte) (redis.Resp, error) {
    if len(args) < 1 {
        return toRespErrorf("len(args) = %d, expect >= 2", len(args))
    }
    keys := args
    var cnt int64 = 0
    bc := c.s.bc

    for _, key := range keys {
        err := bc.Del(key)
        if err != nil {
            return redis.NewInt(0), err
        }
        cnt++
    }
    return redis.NewInt(cnt), nil
}

// PING
func PingCmd(c *conn, args [][]byte) (redis.Resp, error) {
    if len(args) != 0 {
        return toRespErrorf("len(args) = %d, expect = 0", len(args))
    }
    return redis.NewString("PONG"), nil
}

func CommandCmd(c *conn, args [][]byte) (redis.Resp, error) {
    return redis.NewArray(), nil
}

// ROLE
func RoleCmd(c *conn, args [][]byte) (redis.Resp, error) {
    if len(args) != 0 {
        return toRespErrorf("len(args) = %d, expect = 0", len(args))
    }
    arr := redis.NewArray()
    s := c.s
    masterAddr := s.repl.masterAddr.Get()
    if masterAddr == "" {
        // master
        arr.Append(redis.NewBulkBytesWithString("master"))
        s.repl.Lock()
        defer s.repl.Unlock()
        slaves := redis.NewArray()
        for slave, _ := range s.repl.slaves {
            a := redis.NewArray()
            if addr := slave.nc.RemoteAddr(); addr == nil {
                continue
            } else {
                a.Append(redis.NewBulkBytesWithString(strings.Split(addr.String(), ":")[0]))
            }
            // append slave listening port, offset
            slaves.Append(a)
        }
        arr.Append(slaves)
    } else {
        // slave
        arr.Append(redis.NewBulkBytesWithString("slave"))
        seps := strings.Split(masterAddr, ":")
        if len(seps) == 2 {
            port, err := strconv.ParseInt(seps[1], 10, 16)
            if err != nil {
                return toRespError(err)
            }
            arr.Append(redis.NewBulkBytesWithString(seps[0]))
            arr.Append(redis.NewInt(int64(port)))
        } else {
            return toRespErrorf("invalid master addr, must ip:port, but %s", s.repl.masterAddr)
        }
        arr.Append(redis.NewInt(s.repl.syncFileId))
        arr.Append(redis.NewInt(s.repl.syncOffset))
    }
    return arr, nil
}

// INFO [section]
func InfoCmd(c *conn, args [][]byte) (redis.Resp, error) {
    return nil, nil
}

// MERGE
func MergeCmd(c *conn, args [][]byte) (redis.Resp, error) {
    c.s.merge()
    return redis.NewString("OK"), nil
}

// FLUSHALL
func FlushAllCmd(c *conn, args [][]byte) (redis.Resp, error) {
    bc := c.s.bc
    err := bc.ClearAll()
    if err != nil {
        toRespError(err)
    }
    return redis.NewString("OK"), nil
}

func init() {
    Register("command", CommandCmd, CmdReadOnly)
    Register("set", SetCmd, CmdWrite)
    Register("get", GetCmd, CmdReadOnly)
    Register("del", DelCmd, CmdWrite)
    Register("ping", PingCmd, CmdReadOnly)
    Register("role", RoleCmd, CmdReadOnly)
    Register("info", InfoCmd, CmdReadOnly)
    Register("merge", MergeCmd, CmdReadOnly)
    register("flushall", FlushAllCmd, CmdWrite)
}

