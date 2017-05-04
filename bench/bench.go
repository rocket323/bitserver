package main

import (
    "time"
    "bytes"
    "sync/atomic"
    "sync"
    "math/rand"
    "net"
    "log"
    "fmt"
    "flag"
    "strings"
    "hash/crc32"
    "bufio"
    "strconv"

    redis "github.com/reborndb/go/redis/resp"
)

/*
1. readwrite                        : 读写数据
2. readwritewhilemerge              : 读写操作到达一定比例后开始合并数据
3. readwritewhilemigrate            : 读写操作到达一定比例后开始迁移数据
4. readwritemigreatewhilemerge      : 读写操作到达一定比例后开始合并和迁移数据
5. readwritewhilesync               : 主从模式下读写
6. readwritemergewhilesync          : 主从模式下读写，在读写操作到达一定比例后开始合并数据
7. readwritemigratewhilesync        : 主从模式下读写，在读写操作到达一定比例后开始迁移数据
*/

var (
    t string
    num int
    valueSize int
    servers string
    clientNum int
    writePortion int
    randomSpace int
    triggerPortion int

    value []byte
    urls[]string
    slots = make(map[int]bool)
)

const (
    MaxSlotNum = 1024
)

func init() {
    flag.StringVar(&t, "t", "read,write,readwritewhilemerge,readwritewhilemigrate,readwritewhilesync,readwritemigratewhilesync", "test cases")
    flag.IntVar(&num, "num", 10000, "num of operations")
    flag.IntVar(&clientNum, "c", 1, "num of clients per server")
    flag.IntVar(&valueSize, "value_size", 1024, "value size")
    flag.StringVar(&servers, "servers", "127.0.0.1:6379", "servers url")
    flag.IntVar(&writePortion, "write_portion", 50, "write portion")
    flag.IntVar(&randomSpace, "r", 1e9, "key space")
    flag.IntVar(&triggerPortion, "tp", 50, "trigger portion for merge/migrate")
}

func hashTag(key []byte) []byte {
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

func hashTagToSlot(tag []byte) uint32 {
    return crc32.ChecksumIEEE(tag) % MaxSlotNum
}

func hashKeyToSlot(key []byte) ([]byte, uint32) {
    tag := hashTag(key)
    return tag, hashTagToSlot(tag)
}

func randomKey() string {
    return fmt.Sprintf("%012d", rand.Int() % randomSpace)
}

func benchSlaveOf(masterUrl string, slaveUrl string) {
    masterConn := benchGetConn(slaveUrl)
    l := strings.Split(masterUrl, ":")
    masterIp := l[0]
    masterPort, err := strconv.ParseInt(l[1], 10, 32)
    if err != nil {
        log.Fatalf("parse port[%s] failed", l[1])
    }

    err = masterConn.doMustOK("slaveof", masterIp, masterPort)
    if err != nil {
        log.Fatalf("set %s slaveof %s failed, err = %s", slaveUrl, masterUrl, err)
    }
}

type benchConn struct {
    nc net.Conn
}

func (c *benchConn) Close() {
    c.nc.Close()
}

func benchGetConn(url string) *benchConn {
    nc, err := net.Dial("tcp", url)
    if err != nil {
        log.Fatalf("dial server[%s] failed, err = %s", url, err)
    }
    return &benchConn{
        nc: nc,
    }
}

func (c *benchConn) doCmd(cmd string, args ...interface{}) (redis.Resp, error) {
    r := bufio.NewReaderSize(c.nc, 32)
    w := bufio.NewWriterSize(c.nc, 32)

    req := redis.NewRequest(cmd, args...)
    err := redis.Encode(w, req)
    if err != nil {
        return nil, err
    }

    err = w.Flush()
    if err != nil {
        return nil, err
    }

    resp, err := redis.Decode(r)
    if err != nil {
        return nil, err
    }
    return resp, nil
}

func (c *benchConn) doMustOK(cmd string, args ...interface{}) error {
    rsp, err := c.doCmd(cmd, args...);
    if err != nil {
        return err
    }
    x, ok := rsp.(*redis.String);
    if !ok {
        log.Fatalf("not string resp, type[%T]", rsp)
    }
    if x.Value != "OK" {
        log.Fatalf("not ok, but - %s", x.Value)
    }
    return nil
}

var (
    totalOps int32 = 0

    readWriteProc = func(url string, n int, triggerCh chan int, wg *sync.WaitGroup) {
        defer wg.Done()
        conn := benchGetConn(url)
        for i := 0; i < n; i++ {
            key := randomKey()

            x := rand.Int() % 2
            var err error
            if x == 0 { // read
                _, err = conn.doCmd("get", key)
                if err != nil {
                    log.Fatalf("get key[%s] failed, err = %s", key, err)
                }
            } else { // write
                _, slot := hashKeyToSlot([]byte(key))
                slots[int(slot)] = true
                _, err = conn.doCmd("set", key, value)
                if err != nil {
                    log.Fatalf("set key[%s] failed, err = %s", key, err)
                }
            }

            newTotal := atomic.AddInt32(&totalOps, 1)
            if newTotal == int32(int64(num) * int64(triggerPortion) / 100) {
                triggerCh <- 1
            }
        }
    }
    mergeProc = func(url string, triggerCh chan int, wg *sync.WaitGroup) {
        <-triggerCh
        defer wg.Done()

        conn := benchGetConn(url)
        _, err := conn.doCmd("merge")
        if err != nil {
            log.Fatalf("merge failed, err = %s", err)
        }
    }

    migrateProc = func(src_url string, dst_url string, slots map[int]bool, triggerCh chan int, wg *sync.WaitGroup) {
        <-triggerCh
        defer wg.Done()

        // TODO migrate
    }
)

func BenchReadWrite(svr string) {
    triggerCh := make(chan int, 1)
    start := time.Now()
    wg := &sync.WaitGroup{}
    for i := 0; i < clientNum; i++ {
        n := num / clientNum
        if i == 0 {
            n += num % clientNum
        }
        wg.Add(1)
        go readWriteProc(svr, n, triggerCh, wg)
    }
    wg.Wait()
    end := time.Now()
    d := end.Sub(start)

    // report
    fmt.Printf("========\nget/set finish in %.2f seconds\n", d.Seconds())
    fmt.Printf("%.2f qps\n", float64(num) / d.Seconds())
    writeMB := float64(num * valueSize) / 1e6
    fmt.Printf("%.2f MB/s\n", writeMB / d.Seconds())
    fmt.Printf("%.2f micros/op\n", d.Seconds() * 1e6 / float64(num))
}

func BenchReadWriteWhileMerge(svr string) {
    triggerCh := make(chan int, 1)

    wg := &sync.WaitGroup{}
    wg.Add(1)
    go mergeProc(urls[0], triggerCh, wg)

    for i := 0; i < clientNum; i++ {
        n := num / clientNum
        if i == 0 {
            n += num % clientNum
        }
        wg.Add(1)
        go readWriteProc(svr, n, triggerCh, wg)
    }

    wg.Wait()
}

func BenchReadWriteWhileMigrate(src string, dst string) {
    triggerCh := make(chan int, 1)
    wg := &sync.WaitGroup{}
    wg.Add(1)
    go migrateProc(src, dst, slots, triggerCh, wg)

    for i := 0; i < clientNum; i++ {
        n := num / clientNum
        if i == 0 {
            n += num % clientNum
        }
        wg.Add(1)
        go readWriteProc(src, n, triggerCh, wg)
    }
    wg.Wait()
}

func BenchReadWriteMigrateWhileMerge(src string, dst string) {
}

func BenchReadWriteWhileSync() {
    benchSlaveOf(urls[0], urls[1])
    BenchReadWrite(urls[0])
}

func BenchReadWriteMergeWhileSync() {
    benchSlaveOf(urls[0], urls[1])
    BenchReadWriteWhileMerge(urls[0])
}

func BenchReadWriteMigrateWhileSync() {
    benchSlaveOf(urls[0], urls[1])
    benchSlaveOf(urls[2], urls[3])
    BenchReadWriteWhileMigrate(urls[0], urls[2])
}

func main() {
    flag.Parse()
    testcases := strings.Split(t, ",")
    urls = strings.Split(servers, ",|")
    value = make([]byte, valueSize)

    for _, tc := range testcases {
        switch tc {
        case "readwrite":
            BenchReadWrite(urls[0])
        case "readwritewhilemerge":
            BenchReadWriteWhileMerge(urls[0])
        case "readwritewhilemigrate":
            BenchReadWriteWhileMigrate(urls[0], urls[1])
        case "readwritemigreatewhilemerge":
            BenchReadWriteMigrateWhileMerge(urls[0], urls[1])
        case "readwritewhilesync":
            BenchReadWriteWhileSync()
        case "readwritemergewhilesync":
            BenchReadWriteMergeWhileSync()
        case "readwritemigratewhilesync":
            BenchReadWriteMigrateWhileSync()
        default:
            fmt.Printf("unsupported test case [%s]", tc)
        }
    }
}

