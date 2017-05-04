package main

import (
    "sync/atomic"
    "sync"
    "rand"
    "net"
    "log"
    "fmt"
    "flag"
    "strings"

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
)

func init() {
    flag.StringVar(&t, "t", "read,write,readwritewhilemerge,readwritewhilemigrate,readwritewhilesync,readwritemigratewhilesync", "test cases")
    flag.IntVar(&num, "num", 10000, "num of operations")
    flag.IntVar(&c, "c", 1, "num of clients per server")
    flag.IntVar(&valueSize, "value_size", 1024, "value size")
    flag.StringVar(&servers, "servers", "127.0.0.1:6379", "servers url")
    flag.IntVar(&writePortion, "write_portion", 50, "write portion")
    flag.IntVar(&randomSpace, "r", 1e9, "key space")
    flag.IntVar(&triggerPortion, "tp", 50, "trigger portion for merge/migrate")
}

func randomKey() string {
    return fmt.Sprintf("%012d", rand.Int() % randomSpace)
}

func benchSlaveOf(masterUrl string, slaveUrl string) {
    masterConn := benchGetConn(slaveUrl)
    l := strings.Split(masterUrl, ":")
    masterIp := l[0]
    masterPort := strconv.ParseInt(l[1], 10, 32)

    err := masterConn.doCmd("slaveof", masterIp, masterPort)
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
        return err
    }

    err = w.Flush()
    if err != nil {
        return err
    }

    resp, err := redis.Decode(r)
    if err != nil {
        return err
    }
    return resp, nil
}

func (c *benchConn) doMustOK(cmd string, args ...interface{}) error {
    if err := c.doCmd(cmd, args...); err != nil {
        return err
    }
    if rsp, err := c.readLine()
    if err != nil {
        return err
    }
    if strings.ToLower(string(rsp)) != "+ok" {
        return fmt.Errorf("response is not ok but %s", rsp)
    }
    return nil
}

func (c *benchConn) readLine() (line []byte, err error) {
    // if we read too many \n only, maybe something is wrong.
    for i := 0; i < 100; i++ {
        line, err = c.r.ReadSlice('\n')
        if err != nil {
            return nil, err
        } else if line[0] == '\n' {
            // only \n one line, try again
            continue
        }
        break
    }

    i := len(line) - 2
    if i < 0 || line[i] != '\r' {
        return nil, fmt.Errorf("bad resp line terminator")
    }
    return line[:i], nil
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
                err = conn.doCmd("get", key)
                if err != nil {
                    log.Fatalf("get key[%s] failed, err = %s", key, err)
                }
            } else { // write
                err = conn.doCmd("set", key, value)
                if err != nil {
                    log.Fatalf("set key[%s] failed, err = %s", key, err)
                }
            }

            x := atomic.AddInt32(&totalOps, 1)
            if x == int32(int64(num) * int64(triggerPortion) / 100) {
                triggerCh <- 1
            }
        }
    }
    mergeProc = func(url string, triggerCh chan int, wg *sync.WaitGroup) {
        <-triggerCh
        defer wg.Done()

        conn := benchGetConn(url)
        err := conn.doCmd("merge")
        if err != nil {
            log.Fatalf("merge failed, err = %s", err)
        }
    }

    migrateProc = func(src_url string, dst_url string, slots []int, triggerCh chan int, wg *sync.WaitGroup) {
        <-triggerCh
        defer wg.Done()

        conn := benchGetConn(url)
        err := conn.doCmd("migrate")
    }
)

func BenchReadWrite(svr string) {
    start := time.Now()
    wg := &sync.WaitGroup{}
    for i = 0; i < clientNum; i++ {
        n := num / clientNum
        if i == 0 {
            n += num % clientNum
        }
        wg.Add(1)
        go readWriteProc(svr, n, wg)
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

    for i = 0; i < clientNum; i++ {
        n := num / clientNum
        if i == 0 {
            n += num % clientNum
        }
        wg.Add(1)
        go readProc(svr, n, triggerCh, wg)
    }

    wg.Wait()
}

func BenchReadWriteWhileMigrate(src string, dst string) {
    triggerCh := make(chan int, 1)
    wg := &sync.WaitGroup{}
    wg.Add(1)
    go migrateProc(src, dst, slots, triggerCh, wg)

    for i = 0; i < clientNum; i++ {
        n := num / clientNum
        if i == 0 {
            n += num % clientNum
        }
        wg.Add(1)
        go readProc(src, n, triggerCh, wg)
    }
    wg.Wait()
}

func BenchReadWriteMigrateWhileMerge() {
}

func BenchReadWriteWhileSync() {
    benchSlaveOf(urls[0], urls[1])
    BenReadWrite(urls[0])
}

func BenchReadWriteMergeWhileSync() {
    benchSlaveOf(urls[0], urls[1])
    BenchReadWriteWhileMerge(urls[0])
}

func BenchReadWriteMigrateWhileSync() {
    benchSlaveOf(urls[0], urls[1])
    benchSlaveOf(urls[2], urls[3])
    BenchReadWriteMigrateWhileSync(urls[0], urls[2])
}

func main() {
    flag.Parse()
    testcases := strings.Split(t, ",")
    urls = strings.Split(servers, ",|")
    value := make([]byte, value_size)

    for _, tc := range testcases {
        switch tc {
        case "readwrite":
            BenchReadWrite()
        case "readwritewhilemerge":
            BenchReadWriteWhileMerge()
        case "readwritewhilemigrate":
            BenchReadWriteWhileMigrate()
        case "readwritemigreatewhilemerge":
            BenchReadWriteMigrateWhileMerge()
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

