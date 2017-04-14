package bitserver

import (
    "log"
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

type CommandFlag uint32

const (
    CmdWrite CommandFunc = 1 << iota
    CmdReadOnly
)

func Register(name string, f CommandFunc, flag CommandFlag) {
    register(name, f, flag)
}

// GET key
func GetCmd(bc *BitCask, args [][]byte) (redis.Resp, error) {
    if len(args) < 1 {
        return nil, toRespError("len(args) = %d, expect >= 1", len(args))
    }

    key := args[0]

    value, err := bc.Get(string(key))
    if err != nil {
        return toRespError(err)
    } else {
        return redis.NewBulkBytes(value), nil
    }
}

// SET key value [EX seconds]
func SetCmd(bc *BitCask, args [][]byte) (redis.Resp, error) {
    if len(args) < 2 {
        return toRespError("len(args) = %d, expect >= 2", len(args))
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

