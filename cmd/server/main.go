package main

import (
	"flag"
	"path/filepath"
	"time"

	beehiveStorage "github.com/deepfabric/beehive/storage"
	"github.com/deepfabric/beehive/storage/nemo"
	"github.com/deepfabric/busybee/pkg/storage"
	"github.com/deepfabric/prophet"
	"github.com/fagongzi/log"
)

var (
	addr = flag.String("addr", "127.0.0.1:6379", "svr")
	data = flag.String("data", "", "data path")
	wait = flag.Int("wait", 0, "wait")
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
	select {}
}
