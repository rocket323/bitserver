package bitserver

type Config struct {
    Listen      int
    Dbpath      string
}

func DefaultConfig() *Config {
    return &Config{
        Listen: 12345,
        Dbpath: "testdb",
    }
}

