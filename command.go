package bitserver

import (
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

type CommandFunc func(bc *bitcask.BitCask, args [][]byte) (redis.Resp, error)
type CommandFlag uint32

const (
    CmdWrite CommandFlag = 1 << iota
    CmdReadOnly
)

func Register(name string, f CommandFunc, flag CommandFlag) {
    register(name, f, flag)
}

// GET key
func GetCmd(bc *bitcask.BitCask, args [][]byte) (redis.Resp, error) {
    if len(args) < 1 {
        return toRespErrorf("len(args) = %d, expect >= 1", len(args))
    }

    key := args[0]

    value, err := bc.Get(string(key))
    if err != nil && err != bitcask.ErrNotFound {
        return toRespError(err)
    } else {
        return redis.NewBulkBytes(value), nil
    }
}

// SET key value [EX seconds]
func SetCmd(bc *bitcask.BitCask, args [][]byte) (redis.Resp, error) {
    if len(args) < 2 {
        return toRespErrorf("len(args) = %d, expect >= 2", len(args))
    }

    key := args[0]
    value := args[1]

    err := bc.Set(string(key), value)
    if err != nil {
        return toRespError(err)
    } else {
        return redis.NewString("OK"), nil
    }
}

// DEL KEY [KEY ...]
func DelCmd(bc *bitcask.BitCask, args [][]byte) (redis.Resp, error) {
    if len(args) < 1 {
        return toRespErrorf("len(args) = %d, expect >= 2", len(args))
    }
    keys := args
    var cnt int64 = 0

    for _, key := range keys {
        err := bc.Del(string(key))
        if err != nil {
            return redis.NewInt(0), err
        }
        cnt++
    }
    return redis.NewInt(cnt), nil
}

func CommandCmd(bc *bitcask.BitCask, args [][]byte) (redis.Resp, error) {
    return redis.NewArray(), nil
}

func init() {
    Register("set", SetCmd, CmdWrite)
    Register("get", GetCmd, CmdReadOnly)
    Register("del", DelCmd, CmdWrite)
    Register("command", CommandCmd, CmdReadOnly)
}

