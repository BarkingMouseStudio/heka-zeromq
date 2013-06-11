package heka_websockets

import (
	"errors"
	"fmt"
	zmq "github.com/alecthomas/gozmq"
	"github.com/mozilla-services/heka/message"
	"github.com/mozilla-services/heka/pipeline"
)

type ZeroMQInputConfig struct {
	Address string `toml:"address"`
}

type ZeroMQInput struct {
	conf    *ZeroMQInputConfig
	context *zmq.Context
	socket  *zmq.Socket
}

func (zi *ZeroMQInput) ConfigStruct() interface{} {
	return &ZeroMQInputConfig{":4000"}
}

func (zi *ZeroMQInput) Init(config interface{}) error {
	zi.conf = config.(*ZeroMQInputConfig)

	var err error
	if zi.context, err = zmq.NewContext(); err != nil {
		return errors.New(fmt.Sprintf("creating context – %s", err.Error()))
	}
	if zi.socket, err = zi.context.NewSocket(zmq.PULL); err != nil {
		return errors.New(fmt.Sprintf("creating context – %s", err.Error()))
	}
	if err = zi.socket.Bind(zi.conf.Address); err != nil {
		return errors.New(fmt.Sprintf("creating context – %s", err.Error()))
	}

	return nil
}

func (zi *ZeroMQInput) Run(ir pipeline.InputRunner, h pipeline.PluginHelper) error {
	// Get the InputRunner's chan to receive empty PipelinePacks
	packs := ir.InChan()

	var pack *pipeline.PipelinePack
	var count int
	var b []byte
	var err error

	// Read data from websocket broadcast chan
	for {
		b, err = zi.socket.Recv(0)
		if err != nil {
			fmt.Println(err.Error())
			return err
		}

		// Grab an empty PipelinePack from the InputRunner
		pack = <-packs

		// Trim the excess empty bytes
		count = len(b)
		pack.MsgBytes = pack.MsgBytes[:count]

		pack.Message = &message.Message{}
		pack.Decoded = true

		// Copy ws bytes into pack's bytes
		copy(pack.MsgBytes, b)

		// Send pack into Heka pipeline
		ir.Inject(pack)
	}

	return nil
}

func (zi *ZeroMQInput) Stop() {
	zi.socket.Close()
	zi.context.Close()
}

func init() {
	pipeline.RegisterPlugin("ZeroMQInput", func() interface{} {
		return new(ZeroMQInput)
	})
}
