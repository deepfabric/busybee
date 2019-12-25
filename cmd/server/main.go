package main

import (
	"flag"
	"os"
	"os/signal"
	"path/filepath"
	"syscall"
	"time"

	beehiveStorage "github.com/deepfabric/beehive/storage"
	"github.com/deepfabric/beehive/storage/nemo"
	"github.com/deepfabric/busybee/pkg/api"
	"github.com/deepfabric/busybee/pkg/core"
	"github.com/deepfabric/busybee/pkg/notify"
	"github.com/deepfabric/busybee/pkg/storage"
	"github.com/deepfabric/prophet"
	"github.com/fagongzi/log"
)

var (
	addr    = flag.String("addr", "127.0.0.1:8081", "beehive rpc address")
	apiAddr = flag.String("addr-api", "127.0.0.1:8080", "http restful api address")
	data    = flag.String("data", "", "data path")
	wait    = flag.Int("wait", 0, "wait")
)

func main() {
	log.InitLog()
	prophet.SetLogger(log.NewLoggerWithPrefix("[prophet]"))

	if *wait > 0 {
		time.Sleep(time.Second * time.Duration(*wait))
	}

	nemoStorage, err := nemo.NewStorage(filepath.Join(*data, "nemo"))
	if err != nil {
		log.Fatalf("create nemo failed with %+v", err)
	}

	store, err := storage.NewStorage(*addr, *data,
		[]beehiveStorage.MetadataStorage{nemoStorage},
		[]beehiveStorage.DataStorage{nemoStorage})
	if err != nil {
		log.Fatalf("create storage failed with %+v", err)
	}

	err = store.Start()
	if err != nil {
		log.Fatalf("start storage failed with %+v", err)
	}

	notifier := notify.NewQueueBasedNotifier(store)
	engine, err := core.NewEngine(store, notifier)
	if err != nil {
		log.Fatalf("create core engine failed with %+v", err)
	}

	err = engine.Start()
	if err != nil {
		log.Fatalf("start core engine failed with %+v", err)
	}

	apiServer, err := api.NewHTTPServer(*apiAddr, engine)
	if err != nil {
		log.Fatalf("start api server failed with %+v", err)
	}

	go apiServer.Start()

	sc := make(chan os.Signal, 1)
	signal.Notify(sc,
		syscall.SIGHUP,
		syscall.SIGINT,
		syscall.SIGTERM,
		syscall.SIGQUIT)

	sig := <-sc
	apiServer.Stop()
	engine.Stop()
	store.Close()
	log.Infof("exit: signal=<%d>.", sig)
	switch sig {
	case syscall.SIGTERM:
		log.Infof("exit: bye :-).")
		os.Exit(0)
	default:
		log.Infof("exit: bye :-(.")
		os.Exit(1)
	}
}
