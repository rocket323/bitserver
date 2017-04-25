package bitserver

type Config struct {
    Listen      int
    Dbpath      string
}

func NewConfig() *Config {
    return &Config{
        Listen: 12345,
        Dbpath: "testdb",
    }
}

