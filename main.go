// L2met converts a formatted log stream into metrics.
package main

import (
	"flag"
	"fmt"
	"log"
	"net/http"
	"os"
	"runtime"

	"github.com/DataDog/l2met/auth"
	"github.com/DataDog/l2met/conf"
	"github.com/DataDog/l2met/metchan"
	"github.com/DataDog/l2met/outlet"
	"github.com/DataDog/l2met/reader"
	"github.com/DataDog/l2met/receiver"
	"github.com/DataDog/l2met/store"
)

// Hold onto the app's global config.
var cfg *conf.D

func init() {
	cfg = conf.New()
	flag.Parse()
}

func init() {
	runtime.GOMAXPROCS(runtime.NumCPU())
}

func main() {
	if cfg.PrintVersion {
		fmt.Println(conf.Version)
		os.Exit(0)
	}

	// Can be passed to other modules
	// as an internal metrics channel.
	mchan := metchan.New(cfg)
	mchan.Start()

	// The store will be used by receivers and outlets.
	var st store.Store
	if len(cfg.RedisHost) > 0 {
		redisStore := store.NewRedisStore(cfg)
		redisStore.Mchan = mchan
		st = redisStore
		fmt.Printf("at=initialized-redis-store\n")
	} else {
		st = store.NewMemStore()
		fmt.Printf("at=initialized-mem-store\n")
	}

	if cfg.UseLibratoOutlet {
		rdr := reader.New(cfg, st)
		rdr.Mchan = mchan
		outlet := outlet.NewLibratoOutlet(cfg, rdr)
		outlet.Mchan = mchan
		outlet.Start()
	}

	if cfg.UseDataDogOutlet {
		rdr := reader.New(cfg, st)
		rdr.Mchan = mchan
		outlet := outlet.NewDataDogOutlet(cfg, rdr)
		outlet.Mchan = mchan
		outlet.Start()
	}

	if cfg.UsingReciever {
		recv := receiver.NewReceiver(cfg, st)
		recv.Mchan = mchan
		recv.Start()
		http.Handle("/logs", recv)
	}

	http.Handle("/health", st)
	http.HandleFunc("/sign", auth.ServeHTTP)
	e := http.ListenAndServe(fmt.Sprintf(":%d", cfg.Port), nil)
	if e != nil {
		log.Fatal("Unable to start HTTP server.")
	}
	fmt.Printf("at=l2met-initialized port=%d\n", cfg.Port)
}
