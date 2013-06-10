package heka_websockets

import (
	"errors"
	"fmt"
	zmq "github.com/alecthomas/gozmq"
	"github.com/mozilla-services/heka/pipeline"
)

type ZeroMQOutputConfig struct {
	Address string `toml:"address"`
}

type ZeroMQOutput struct {
	conf    *ZeroMQOutputConfig
	context *zmq.Context
	socket  *zmq.Socket
}

func (zo *ZeroMQOutput) ConfigStruct() interface{} {
	return &ZeroMQOutputConfig{":5000"}
}

func (zo *ZeroMQOutput) Init(config interface{}) error {
	zo.conf = config.(*ZeroMQOutputConfig)

	var err error
	if zo.context, err = zmq.NewContext(); err != nil {
		return errors.New(fmt.Sprintf("creating context – %s", err.Error()))
	}
	if zo.socket, err = zo.context.NewSocket(zmq.PUB); err != nil {
		return errors.New(fmt.Sprintf("creating socket – %s", err.Error()))
	}
	if err = zo.socket.Bind(zo.conf.Address); err != nil {
		return errors.New(fmt.Sprintf("binding socket – %s", err.Error()))
	}

	return nil
}

func (zo *ZeroMQOutput) Run(or pipeline.OutputRunner, h pipeline.PluginHelper) error {
	defer func() {
		zo.socket.Close()
		zo.context.Close()
	}()

	var b []byte
	var p [][]byte
	for pc := range or.InChan() {
		b = pc.Pack.MsgBytes
		p = [][]byte{nil, b}
		zo.socket.SendMultipart(p, 0)
		pc.Pack.Recycle()
	}

	return nil
}

func init() {
	pipeline.RegisterPlugin("ZeroMQOutput", func() interface{} {
		return new(ZeroMQOutput)
	})
}
