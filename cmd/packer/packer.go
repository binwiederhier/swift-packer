package main

import (
	"flag"
	"fmt"
	"heckel.io/swift/packer"
	"log"
	"os"
)

func main() {
	listenAddr := flag.String("listen", ":1234", "Listen address for packer service")
	minSize := flag.Int("minsize", 128 * 1024, "Minimum pack size in bytes")
	maxWait := flag.Int("maxwait", 100, "Wait time in milliseconds for downstream PUTs before closing pack")
	debug := flag.Bool("debug", false, "Enable debug mode")
	quiet := flag.Bool("quiet", false, "Do not output anything")
	flag.Parse()

	if flag.NArg() < 1 {
		fmt.Println("Syntax: packer [OPTIONS] HOST:PORT")
		fmt.Println("Options:")
		flag.PrintDefaults()
		os.Exit(1)
	}

	forwardAddr := flag.Arg(0)

	packer.Quiet = *quiet
	packer.Debug = *debug

	packer := packer.NewPacker(&packer.Config{
		ListenAddr:  *listenAddr,
		ForwardAddr: forwardAddr,
		MinSize:     *minSize,
		MaxWait:     *maxWait,
	})

	log.Fatalln(packer.ListenAndServe())
}
