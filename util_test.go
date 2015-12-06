package go_rafting

import (
	log "github.com/Sirupsen/logrus"
	"reflect"
	"testing"
	"time"
)

func init() {
	log.SetLevel(log.DebugLevel)
}

//const MAX_TIME = time.Time{sec: 1, nsec}

func TestMaxTime(t *testing.T) {
	//	January 1, year 1 00:00:00 UTC
	//	then := time.Date(
	//		2009, 11, 17, 20, 34, 58, 651387237, time.UTC)
	then := time.Date(
		1, 1, 1, 0, 0, 0, 0, time.UTC)
	log.Debugf("then %v", then)
	const maxDuration time.Duration = 1<<63 - 1
	then = then.Add(maxDuration)
	log.Debugf("then %v", then)
	log.Debugf("typeName %v", reflect.TypeOf(RequestVoteResponse{}).Name())
	log.Debug("Debug")
	log.Info("Info")
	log.Warn("Warn")
	//	server := NewServer("server1", []string{"server2", "server3"})
	//	fmt.Printf("server %v created\n", server)
	//	server.StartNewElection()
	//	fmt.Printf("server %v created\n", server)
}
