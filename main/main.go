package main

import (
	"bytes"
	"encoding/gob"
	"flag"
	"fmt"
	logger "github.com/Sirupsen/logrus"
	"github.com/barakb/grafting"
	"github.com/vharitonsky/iniflags"
)

func init() {
	logger.SetLevel(logger.InfoLevel)
}

var (
	quorum = flag.String("quorum", "", "String seperated servers list")
	id     = flag.String("id", "", "server id, have to be one of the quorum")
)

func main() {
	iniflags.Parse()
	logger.Infof("server id:%s, quorum:%s", *id, *quorum)
	fmt.Printf("%#v\n", "foo")
	gob.Register(grafting.AppendEntries{})
	gob.Register(grafting.AppendEntriesResponse{})
	var network bytes.Buffer
	enc := gob.NewEncoder(&network)
	interfaceEncode(enc, grafting.AppendEntries{Term: 1})
	dec := gob.NewDecoder(&network)
	result := interfaceDecode(dec)
	fmt.Printf("%#v\n", result)
}

func interfaceEncode(enc *gob.Encoder, m grafting.Message) {
	err := enc.Encode(&m)
	if err != nil {
		logger.Fatal("encode:", err)
	}
}

func interfaceDecode(dec *gob.Decoder) grafting.Message {
	// The decode will fail unless the concrete type on the wire has been
	// registered. We registered it in the calling function.
	var m grafting.Message
	err := dec.Decode(&m)
	if err != nil {
		logger.Fatal("decode:", err)
	}
	return m
}
