package bitserver

// SLAVEOF host port
func SlaveOfCmd(c *conn, args [][]byte) (redis.Resp, error) {
    if len(args) != 2 {
        return toRespErrorf("len(args) = %d, expect = 2", len(args))
    }
    addr := fmt.Sprintf("%s:%s", string(args[0]), string(args[1]))
    log.Printf("set slave of %s", addr)

}

func init() {
    Register("slaveof", SlaveofCmd, CmdReadOnly)
}

