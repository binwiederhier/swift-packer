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
	maxWait := flag.Int("maxwait", 200, "Wait time in milliseconds for downstream PUTs before closing pack")
	flag.Parse()

	if flag.NArg() < 1 {
		fmt.Println("Syntax: packer [OPTIONS] HOST:PORT")
		fmt.Println("Options:")
		flag.PrintDefaults()
		os.Exit(1)
	}

	forwardAddr := flag.Arg(0)

	packer := packer.NewPacker(&packer.Config{
		ListenAddr:  *listenAddr,
		ForwardAddr: forwardAddr,
		MinSize:     *minSize,
		MaxWait:     *maxWait,
	})

	log.Fatalln(packer.ListenAndServe())
}
