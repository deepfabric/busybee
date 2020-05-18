package main

import (
	"flag"
	"fmt"
	"net/http"
	"os"
	"os/signal"
	"path/filepath"
	"runtime"
	"syscall"
	"time"

	beehiveStorage "github.com/deepfabric/beehive/storage"
	"github.com/deepfabric/beehive/storage/nemo"
	"github.com/deepfabric/busybee/pkg/api"
	"github.com/deepfabric/busybee/pkg/core"
	"github.com/deepfabric/busybee/pkg/notify"
	"github.com/deepfabric/busybee/pkg/storage"
	"github.com/deepfabric/busybee/pkg/util"
	"github.com/deepfabric/prophet"
	"github.com/fagongzi/log"
)

var (
	addr       = flag.String("addr", "127.0.0.1:8081", "beehive api address")
	addrPPROF  = flag.String("addr-pprof", "", "pprof")
	ck         = flag.String("addr-ck", "", "ck address")
	ckUser     = flag.String("ck-user", "", "ck user")
	ckPassword = flag.String("ck-pass", "", "ck pass")
	data       = flag.String("data", "", "data path")
	stores     = flag.Uint64("stores", 1, "Number of store count.")
	wait       = flag.Int("wait", 0, "wait")
	version    = flag.Bool("version", false, "Show version info")
)

var (
	stopping = false
)

func main() {
	flag.Parse()
	if *version {
		util.PrintVersion()
		os.Exit(0)
	}

	log.InitLog()
	prophet.SetLogger(log.NewLoggerWithPrefix("[prophet]"))

	if *wait > 0 {
		time.Sleep(time.Second * time.Duration(*wait))
	}

	if *addrPPROF != "" {
		runtime.SetBlockProfileRate(1)
		go func() {
			log.Errorf("start pprof failed, errors:\n%+v",
				http.ListenAndServe(*addrPPROF, nil))
		}()
	}

	var metaStores []beehiveStorage.MetadataStorage
	var dataStores []beehiveStorage.DataStorage

	for i := uint64(0); i < *stores; i++ {
		store, err := nemo.NewStorage(filepath.Join(*data, fmt.Sprintf("nemo-%d", i)))
		if err != nil {
			log.Fatalf("create nemo failed with %+v", err)
		}

		metaStores = append(metaStores, store)
		dataStores = append(dataStores, store)
	}

	store, err := storage.NewStorage(*data, metaStores, dataStores)
	if err != nil {
		log.Fatalf("create storage failed with %+v", err)
	}

	notifier := notify.NewQueueBasedNotifier(store)
	engine, err := core.NewEngine(store, notifier,
		core.WithClickhouse(*ck, *ckUser, *ckPassword))
	if err != nil {
		log.Fatalf("create core engine failed with %+v", err)
	}

	err = engine.Start()
	if err != nil {
		log.Fatalf("start core engine failed with %+v", err)
	}

	apiServer, err := api.NewAPIServer(*addr, engine)
	if err != nil {
		log.Fatalf("start api server failed with %+v", err)
	}

	go apiServer.Start()

	sc := make(chan os.Signal, 2)
	signal.Notify(sc,
		syscall.SIGHUP,
		syscall.SIGINT,
		syscall.SIGTERM,
		syscall.SIGQUIT)

	for {
		sig := <-sc

		if !stopping {
			stopping = true
			go func() {
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
			}()
			continue
		}

		log.Infof("exit: bye :-).")
		os.Exit(0)
	}
}
