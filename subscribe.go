package mqtt

import (
	"context"
	"time"

	paho "github.com/eclipse/paho.mqtt.golang"
	"go.k6.io/k6/js/common"
	"go.k6.io/k6/js/modules"
	"go.k6.io/k6/lib"
)

func init() {
	modules.Register("k6/x/mqtt", new(Mqtt))
}

// Subscribe to the given topic return a channel to wait the message
func (*Mqtt) Subscribe(
	ctx context.Context,
	// Mqtt client to be used
	client paho.Client,
	// Topic to consume messages from
	topic string,
	// The QoS of messages
	qos,
	// timeout ms
	timeout uint,
) chan paho.Message {
	state := lib.GetState(ctx)
	if state == nil {
		common.Throw(common.GetRuntime(ctx), ErrorState)
		return nil
	}
	received := make(chan paho.Message)
	messageCB := func(client paho.Client, msg paho.Message) {
		go func(msg paho.Message) {
			received <- msg
		}(msg)
	}
	if client == nil {
		common.Throw(common.GetRuntime(ctx), ErrorClient)
		return nil
	}
	token := client.Subscribe(topic, byte(qos), messageCB)
	if !token.WaitTimeout(time.Duration(timeout) * time.Millisecond) {
		common.Throw(common.GetRuntime(ctx), ErrorTimeout)
		return nil
	}
	err := token.Error()
	if err != nil {
		common.Throw(common.GetRuntime(ctx), ErrorTimeout)
		return nil
	}
	return received
}

// Consume will wait for one message to arrive
func (*Mqtt) Consume(
	ctx context.Context,
	token chan paho.Message,
	// timeout ms
	timeout uint,
) string {
	state := lib.GetState(ctx)
	if state == nil {
		common.Throw(common.GetRuntime(ctx), ErrorState)
		return ""
	}
	if token == nil {
		common.Throw(common.GetRuntime(ctx), ErrorConsumeToken)
		return ""
	}

	select {
	case msg := <-token:
		return string(msg.Payload())
	case <-time.After(time.Millisecond * time.Duration(timeout)):
		common.Throw(common.GetRuntime(ctx), ErrorTimeout)
		return ""
	}
}

// ConsumeBytes will wait for one message to arrive in binary format
func (*Mqtt) ConsumeBytes(
	ctx context.Context,
	token chan paho.Message,
	// timeout ms
	timeout uint,
) []byte {
	state := lib.GetState(ctx)
	if state == nil {
		common.Throw(common.GetRuntime(ctx), ErrorState)
		return nil
	}
	if token == nil {
		common.Throw(common.GetRuntime(ctx), ErrorConsumeToken)
		return nil
	}

	select {
	case msg := <-token:
		//log.Println("consume", string(msg.Payload()))
		return msg.Payload()
	case <-time.After(time.Millisecond * time.Duration(timeout)):
		common.Throw(common.GetRuntime(ctx), ErrorTimeout)
		return nil
	}
}
