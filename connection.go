package grafting

import (
	"bufio"
	"io"
	"net"
	"time"
)

type Transport interface {
	io.Reader
	io.Writer
	Closer
	SetWriteDeadline(t time.Time) error
}

type TransportFactory func(address string) (Transport, error)

func TCPTransportFactory(address string) (Transport, error) {
	return net.Dial("tcp", address)
}

type connection struct {
	transport    Transport
	encoder      Encoder
	decoder      Decoder
	closed       bool
	to           string
	initiateByMe bool
	writeTimeout time.Duration
	bufWriter    *bufio.Writer
}

func newConnection(transport Transport, encoderDecoderBuilder EncoderDecoderBuilder, initiateByMe bool, writeTimeout time.Duration) *connection {
	bufWriter := bufio.NewWriter(transport)
	return &connection{transport, encoderDecoderBuilder.Encoder(bufWriter), encoderDecoderBuilder.Decoder(transport), false, "", initiateByMe, writeTimeout, bufWriter}
}

func (c connection) Close() error {
	c.closed = true
	if c.initiateByMe {
		return c.transport.Close()
	}
	return nil
}

func (c connection) Encode(e interface{}) error {
	if err := c.encoder.Encode(e); err != nil {
		return err
	}
	if err := c.transport.SetWriteDeadline(time.Now().Add(c.writeTimeout)); err != nil {
		return err
	}
	if err := c.bufWriter.Flush(); err != nil {
		return err
	}
	return nil
}

func (c connection) Decode(e interface{}) error {
	return c.decoder.Decode(e)
}

func (c connection) To() string {
	return c.to
}
