package go_rafting

import (
	"fmt"
	"testing"
)

func TestCreateServer(t *testing.T) {
	server := NewServer("server1", []string{"server2", "server3"})
	fmt.Printf("server %v created\n", server)
	server.StartNewElection()
	fmt.Printf("server %v created\n", server)
}
