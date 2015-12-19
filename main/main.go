package main

import (
	"flag"
	"github.com/vharitonsky/iniflags"
)

var (
	flag1 = flag.String("flag1", "default1", "Description1")

	flagN = flag.Int("flagN", 123, "DescriptionN")
)

func main() {
	iniflags.Parse()

}
