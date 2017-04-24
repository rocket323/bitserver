package bitserver

type Config struct {
    Listen      string
}

func NewConfig() *Config {
    return &Config{
    }
}

