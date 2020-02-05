package version

import (
	"flag"
	"fmt"
	"os"
)

var (
	show = flag.Bool("version", false, "Show version")
)

// set on build time
var (
	GitCommit = ""
	BuildTime = ""
	GoVersion = ""
	Version   = ""
)

// PrintVersion Print out version information
func PrintVersion() {
	if !flag.Parsed() {
		flag.Parse()
	}

	if *show {
		fmt.Println("Version  : ", Version)
		fmt.Println("GitCommit: ", GitCommit)
		fmt.Println("BuildTime: ", BuildTime)
		fmt.Println("GoVersion: ", GoVersion)
		os.Exit(0)
	}
}
