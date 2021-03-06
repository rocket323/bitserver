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
    // flag var
    t string
    num int
    valueSize int
    servers string
    clientNum int
    writePortion int
    randomSpace int
    triggerPortion int

    // normal var
    value []byte
    urls[]string
    slots = make(map[int]bool)

    mu sync.Mutex
    readWriteBegin time.Time
    readWriteEnd time.Time
)

const (
    MaxSlotNum = 1024
)

func init() {
    flag.StringVar(&t, "t",
        "readwrite,readwritewhilemerge,readwritewhilemigrate,readwritemigreatewhilemerge," +
        "readwritewhilesync,readwritemergewhilesync,readwritemigratewhilesync",
        "test cases")
    flag.IntVar(&num, "num", 1000000, "num of operations")
    flag.IntVar(&clientNum, "c", 50, "num of clients per server")
    flag.IntVar(&valueSize, "value_size", 1024, "value size")
    flag.StringVar(&servers, "servers", "127.0.0.1:6379", "servers url")
    flag.IntVar(&writePortion, "write_portion", 50, "write portion")
    flag.IntVar(&randomSpace, "r", 1e9, "key space")
    flag.IntVar(&triggerPortion, "tp", 50, "trigger portion for merge/migrate")

    log.SetFlags(log.LstdFlags | log.Lshortfile)
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

func splitIpPort(url string) (string, int) {
    l := strings.Split(url, ":")
    ip := "127.0.0.1"
    if len(l[0]) > 0 {
        ip = l[0]
    }
    port, err := strconv.ParseInt(l[1], 10, 32)
    if err != nil {
        return "", 0
    }
    return ip, int(port)
}

func benchSlaveOf(masterUrl string, slaveUrl string) {
    masterConn := benchGetConn(slaveUrl)
    masterIp, masterPort := splitIpPort(masterUrl)

    err := masterConn.doMustOK("slaveof", masterIp, masterPort)
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

func decodeIntArray(resp redis.Resp) []int64 {
    v, ok := resp.(*redis.Array)
    if !ok {
        log.Fatalf("migrate resp not IntArray, but %T", resp)
    }

    arr := make([]int64, 0)
    for _, vv := range v.Value {
        b, ok := vv.(*redis.Int)
        if !ok {
            log.Fatalf("migrate resp element not Int, but %T", vv)
        }
        arr = append(arr, int64(b.Value))
    }
    if len(arr) == 0 {
        log.Fatalf("migrate resp has zero length")
    }
    return arr
}

var (
    totalOps int32 = 0

    readWriteProc = func(url string, n int, triggerCh chan int, wg *sync.WaitGroup) {
        defer wg.Done()
        conn := benchGetConn(url)
        writeOps := 0
        readOps := 0
        for i := 0; i < n; i++ {
            key := randomKey()
            x := rand.Int() % 100
            var err error
            if x < writePortion { // write
                writeOps++
                _, slot := hashKeyToSlot([]byte(key))
                slots[int(slot)] = true
                _, err = conn.doCmd("set", key, value)
                if err != nil {
                    log.Fatalf("set key[%s] failed, err = %s", key, err)
                }
            } else { // read
                readOps++
                _, err = conn.doCmd("get", key)
                if err != nil {
                    log.Fatalf("get key[%s] failed, err = %s", key, err)
                }
            }

            newTotal := atomic.AddInt32(&totalOps, 1)
            if newTotal == int32(int64(num) * int64(triggerPortion) / 100) {
                triggerCh <- 1
            }
        }

        // report
        end := time.Now()
        mu.Lock()
        if end.After(readWriteEnd) {
            readWriteEnd = end
        }
        mu.Unlock()
    }
    mergeProc = func(url string, wg *sync.WaitGroup) {
        defer wg.Done()
        begin := time.Now()
        fmt.Printf("merge start...\n")
        conn := benchGetConn(url)
        _, err := conn.doCmd("merge")
        if err != nil {
            log.Fatalf("merge failed, err = %s", err)
        }

        // report
        end := time.Now()
        d := end.Sub(begin)
        var report string
        report += fmt.Sprintf("========\nmerge finish in %.2f seconds\n", d.Seconds())
        fmt.Printf(report)
    }

    migrateProc = func(src_url string, dst_url string, slots map[int]bool, wg *sync.WaitGroup) {
        defer wg.Done()
        begin := time.Now()
        fmt.Printf("migrate from[%s] to [%s] start...\n", src_url, dst_url)

        conn := benchGetConn(src_url)
        dstIp, dstPort := splitIpPort(dst_url)

        cnt := 0
        for slot, _ := range slots {
            for {
                rsp, err := conn.doCmd("slotsmgrtslot", dstIp, dstPort, 1000, slot)
                if err != nil {
                    log.Fatalf("mgrt failed, err = %s", err)
                }
                arr := decodeIntArray(rsp)
                if arr[0] == 0 {
                    break
                }
            }
            cnt++
            log.Printf("migrated slots[%d/%d]", cnt, len(slots))
        }

        // report
        end := time.Now()
        d := end.Sub(begin)
        var report string
        report += fmt.Sprintf("========\nmigrate finish in %.2f seconds\n", d.Seconds())
        fmt.Printf(report)
    }
)

func reportReadWrite() {
    d := readWriteEnd.Sub(readWriteBegin)
    writeMB := float64(num * valueSize) / 1e6
    var report string
    report += fmt.Sprintf("========\nread/write finish in %.2f seconds\n", d.Seconds())
    report += fmt.Sprintf("%.2f qps\n", float64(num) / d.Seconds())
    report += fmt.Sprintf("%.2f MB/s\n", writeMB / d.Seconds())
    report += fmt.Sprintf("%.2f micros/op\n", d.Seconds() * 1e6 / float64(num))
    fmt.Printf(report)
}

func benchReadWrite(svr string) {
    triggerCh := make(chan int, 1)
    wg := &sync.WaitGroup{}
    readWriteBegin = time.Now()
    readWriteEnd = time.Now()
    for i := 0; i < clientNum; i++ {
        n := num / clientNum
        if i == 0 {
            n += num % clientNum
        }
        wg.Add(1)
        go readWriteProc(svr, n, triggerCh, wg)
    }
    wg.Wait()
    reportReadWrite()
}

func benchReadWriteWhileMerge(svr string) {
    triggerCh := make(chan int, 1)

    wg := &sync.WaitGroup{}
    wg.Add(1)
    go func () {
        <-triggerCh
        mergeProc(svr, wg)
    }()

    readWriteBegin = time.Now()
    readWriteEnd = time.Now()
    for i := 0; i < clientNum; i++ {
        n := num / clientNum
        if i == 0 {
            n += num % clientNum
        }
        wg.Add(1)
        go readWriteProc(svr, n, triggerCh, wg)
    }

    wg.Wait()
    reportReadWrite()
}

func benchReadWriteWhileMigrate(src string, dst string) {
    triggerCh := make(chan int, 1)
    wg := &sync.WaitGroup{}
    wg.Add(1)
    go func() {
        <-triggerCh
        migrateProc(src, dst, slots, wg)
    }()

    readWriteBegin = time.Now()
    readWriteEnd = time.Now()
    for i := 0; i < clientNum; i++ {
        n := num / clientNum
        if i == 0 {
            n += num % clientNum
        }
        wg.Add(1)
        go readWriteProc(src, n, triggerCh, wg)
    }
    wg.Wait()
    reportReadWrite()
}

func benchReadWriteMigrateWhileMerge(src string, dst string) {
    triggerCh := make(chan int, 1)
    wg := &sync.WaitGroup{}
    wg.Add(2)
    go func() {
        <-triggerCh
        mergeProc(src, wg)
        migrateProc(src, dst, slots, wg)
    }()

    readWriteBegin = time.Now()
    readWriteEnd = time.Now()
    for i := 0; i < clientNum; i++ {
        n := num / clientNum
        if i == 0 {
            n += num % clientNum
        }
        wg.Add(1)
        go readWriteProc(src, n, triggerCh, wg)
    }
    wg.Wait()
    reportReadWrite()
}

func BenchReadWrite(svr string) {
    fmt.Printf("bench readwrite...\n")
    benchReadWrite(svr)
}

func BenchReadWriteWhileMerge(svr string) {
    fmt.Printf("bench readwritewhilemerge...\n")
    benchReadWriteWhileMerge(svr)
}

func BenchReadWriteWhileMigrate(src string, dst string) {
    fmt.Printf("bench readwritewhilemigrate...\n")
    benchReadWriteWhileMigrate(src, dst)
}

func BenchReadWriteMigrateWhileMerge(src string, dst string) {
    fmt.Printf("bench readwritemigratewhilemerge...\n")
    benchReadWriteMigrateWhileMerge(src, dst)
}

func BenchReadWriteWhileSync() {
    fmt.Printf("bench readwritewhilesync...\n")
    benchSlaveOf(urls[0], urls[1])
    BenchReadWrite(urls[0])
}

func BenchReadWriteMergeWhileSync() {
    fmt.Printf("bench readwritemergewhilesync...\n")
    benchSlaveOf(urls[0], urls[1])
    BenchReadWriteWhileMerge(urls[0])
}

func BenchReadWriteMigrateWhileSync() {
    fmt.Printf("bench readwritemigratewhilesync...\n")
    benchSlaveOf(urls[0], urls[1])
    benchSlaveOf(urls[2], urls[3])
    BenchReadWriteWhileMigrate(urls[0], urls[2])
}

func main() {
    flag.Parse()
    testcases := strings.Split(t, ",")
    urls = strings.Split(servers, ",")
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

