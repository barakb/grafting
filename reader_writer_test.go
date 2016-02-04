package grafting

import (
	"bytes"
	logger "github.com/Sirupsen/logrus"
	"testing"
)

func init() {
	logger.SetLevel(logger.InfoLevel)
}

type CloseableBuffer struct {
	bytes.Buffer
}

func (b *CloseableBuffer) Close() error {
	return nil
}

var msg = RequestVoteResponse{Msg: Msg{F: "foo", T: "bar"}, Term: 1, Granted: true}

func TestReaderWriter(t *testing.T) {
	underline := &CloseableBuffer{}
	rw := NewReaderWriter(underline)

	err := rw.Write(msg)
	if err != nil {
		t.Errorf("Failed to write %v, error is: %v", msg, err)
	}
	response, err := rw.Read()
	if err != nil {
		t.Errorf("Failed to read response error is: %v", err)
	}

	if response.From() != "foo" {
		t.Errorf("Wrong response: %v", msg)
	}
}
