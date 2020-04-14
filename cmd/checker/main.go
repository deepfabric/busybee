package main

import (
	"flag"

	"github.com/deepfabric/beehive/storage/nemo"
	"github.com/fagongzi/log"
)

var (
	data = flag.String("data", "", "data path")
)

func main() {
	flag.Parse()

	log.Infof("on %s", *data)
	s, err := nemo.NewStorage(*data)
	if err != nil {
		log.Fatalf("create nemo failed with %+v", err)
	}

	err = s.Scan(nil, nil, func(key, value []byte) (bool, error) {
		log.Infof("************** key: %+v", key)
		return true, nil
	}, false)
	if err != nil {
		log.Fatalf("scan nemo failed with %+v", err)
	}
}
