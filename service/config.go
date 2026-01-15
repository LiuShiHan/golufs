package main

import (
	"time"
)

type ServerConfig struct {
	Port         int
	DataDir      string
	MaxChunkSize int
	ReadTimeout  time.Duration

	Discovery struct {
		Enable    bool
		Type      string
		EndPoints []string
		TTL       int
	}

	Metrics struct {
		Enable bool
		Port   int
		Path   string
	}

	Security struct {
		TLS struct {
			Enable   bool
			CertFile string
			KeyFile  string
			CAFile   string
		}

		Auth struct {
			Enable bool
			Type   string
			Token  string
		}
	}
}

//func main() {
//	absResolverPath, err := filepath.Abs("D:\\workhaha\\golufs\\service\\config.go")
//	fmt.Println(absResolverPath)
//}
