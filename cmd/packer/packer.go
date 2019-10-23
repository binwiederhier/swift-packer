package main

import (
	"errors"
	"flag"
	"fmt"
	"heckel.io/swift/packer"
	"log"
	"os"
	"regexp"
	"strconv"
	"strings"
)

func main() {
	listenAddrFlag := flag.String("listen", ":1234", "Listen address for packer service")
	minSizeFlag := flag.String("minsize", "128k", "Minimum pack size")
	maxWaitFlag := flag.Int("maxwait", 100, "Wait time in milliseconds for downstream PUTs before closing pack")
	debugFlag := flag.Bool("debug", false, "Enable debug mode")
	quietFlag := flag.Bool("quiet", false, "Do not output anything")
	flag.Parse()

	if flag.NArg() < 1 {
		exit(1, "Missing forwarding address / port")
	}

	minSize, err := convertToBytes(*minSizeFlag)
	if err != nil {
		exit(2, "Invalid min size value: " + err.Error())
	}

	forwardAddr := flag.Arg(0)

	packer.Quiet = *quietFlag
	packer.Debug = *debugFlag

	packer := packer.NewPacker(&packer.Config{
		ListenAddr:  *listenAddrFlag,
		ForwardAddr: forwardAddr,
		MinSize:     minSize,
		MaxWait:     *maxWaitFlag,
	})

	log.Fatalln(packer.ListenAndServe())
}

func exit(code int, message string) {
	fmt.Println("Error: " + message)
	fmt.Println("Syntax: packer [OPTIONS] HOST:PORT")
	fmt.Println("Options:")
	flag.PrintDefaults()
	os.Exit(code)
}


func convertToBytes(s string) (int, error) {
	r := regexp.MustCompile(`^(\d+)([bBkKmMgGtT])?$`)
	matches := r.FindStringSubmatch(s)

	if matches == nil {
		return 0, errors.New("cannot convert to bytes: " + s)
	}

	value, err := strconv.Atoi(matches[1])
	if err != nil {
		return 0, err
	}

	unit := strings.ToLower(matches[2])
	switch unit {
	case "k":
		return value * (1 << 10), nil
	case "m":
		return value * (1 << 20), nil
	case "g":
		return value * (1 << 30), nil
	case "t":
		return value * (1 << 40), nil
	default:
		return value, nil
	}
}