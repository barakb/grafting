package grafting

import (
	"bufio"
	"encoding/gob"
	"fmt"
	"io"
)

type MessageReader interface {
	Read() (m Message, err error)
}

type MessageWriter interface {
	Write(m Message) (err error)
}

type MessageReaderWriterCloser interface {
	MessageReader
	MessageWriter
	Closer
}

// a class that can read/write messages
type ReaderWriter struct {
	underline io.ReadWriteCloser
	buffered  *bufio.Writer
	encoder   Encoder
	decoder   Decoder
}

func (rw *ReaderWriter) Write(m Message) (err error) {
	err = rw.encoder.Encode(&m)
	if err == nil {
		fmt.Printf("wrote message %#v and flush\n", m)
		rw.buffered.Flush()
	}
	return err
}

func (rw *ReaderWriter) Read() (Message, error) {
	var m Message
	err := rw.decoder.Decode(&m)
	return m, err
}

func (rw *ReaderWriter) Close() (err error) {
	rw.underline.Close()
	return nil
}

func NewReaderWriter(underline io.ReadWriteCloser) MessageReaderWriterCloser {
	buffered := bufio.NewWriter(underline)
	encoder := gob.NewEncoder(buffered)
	decoder := gob.NewDecoder(underline)
	return &ReaderWriter{underline, buffered, encoder, decoder}
}
