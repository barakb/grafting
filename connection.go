package grafting

import (
	"bufio"
	"io"
	"net"
	"regexp"
	"time"
	//	logger "github.com/Sirupsen/logrus"
)

type Transport interface {
	io.Reader
	io.Writer
	Closer
	SetWriteDeadline(t time.Time) error
}

type TransportFactory func(address string) (Transport, error)

func TCPTransportFactory(address string) (Transport, error) {
	re, err := regexp.Compile("([^:]+:[0-9]+):(.+)")
	if err == nil {
		res := re.FindStringSubmatch(address)
		if len(res) == 3 {
			return net.Dial("tcp", res[1])
		}
	}
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

func newConnection(transport Transport, to string, encoderDecoderBuilder EncoderDecoderBuilder, initiateByMe bool, writeTimeout time.Duration) *connection {
	bufWriter := bufio.NewWriter(transport)
	return &connection{transport, encoderDecoderBuilder.Encoder(bufWriter), encoderDecoderBuilder.Decoder(transport), false, to, initiateByMe, writeTimeout, bufWriter}
}

func (c connection) Close() error {
	c.closed = true
	if c.initiateByMe {
		return c.transport.Close()
	}
	return nil
}

func (c connection) Encode(e interface{}) error {
	//	logger.Infof("connection to %s about to send %#v", c.To(), e)
	if err := c.encoder.Encode(e); err != nil {
		//		logger.Infof("connection to %s send %#v result in encode error", c.To(), e, err)
		return err
	}
	if err := c.transport.SetWriteDeadline(time.Now().Add(c.writeTimeout)); err != nil {
		return err
	}
	if err := c.bufWriter.Flush(); err != nil {
		//		logger.Infof("connection to %s send %#v result in write error", c.To(), e, err)
		return err
	}
	//	logger.Infof("connection to %s send %#v done", c.To(), e)
	return nil
}

func (c connection) Decode(e interface{}) error {
	return c.decoder.Decode(e)
}

func (c connection) To() string {
	return c.to
}
