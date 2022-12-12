package main

import (
	"context"
	"flag"
	"fmt"
	"log"
	"math/rand"
	"runtime"
	"time"

	"github.com/streadway/amqp"

	"github.com/Graphmasters/occamy"
)

const (
	AMQPURL         string = "amqp://localhost:5672" // AMQPURL is the address of the AMQP server.
	RequestExchange string = "service_name-request"  // RequestExchange is the exchange for request messages.
	ControlExchange string = "service_name-control"  // ControlExchange is the exchange for control mesagges.
	HeaderKeyTaskID string = "task-id"               // HeaderKeyTaskID is the key whose value in the header corresponds to the task ID.
)

// main starts an occamy server handling AMQP messages. The reader is expected
// to already be familiar with AMQP.
//
// To communicate to this application messages should be set to either the
// request or control exchange. It is recommended against sending messages
// directly to the queues.
//
// This application is designed to run forever and fail if one thing goes wrong.
// In practice one would implement graceful handling of errors and graceful
// shutdown logic.
//
// *IMPORTANT* The AMQP library used here does NOT implement any reconnection
// logic. Implementing reconnection logic is highly recommended.
func main() {
	slots := flag.Int("slots", runtime.NumCPU(), "the number of slots/maximum number of concurrent tasks")
	flag.Parse()

	server := NewAMQPServer(occamy.ServerConfig{
		Slots:           *slots,
		KillTimeout:     time.Millisecond,
		HeaderKeyTaskID: HeaderKeyTaskID,
		Handler:         handle,
		Monitors:        occamy.Monitors{},
	})

	connection := createConnection(AMQPURL)
	declareExchanges(connection, RequestExchange, ControlExchange)

	go consumeControlMessages(server, connection)
	consumeRequestMessages(server, connection, *slots)
}

// region AMQP Message Wrapper

// AMQPMessage is a wrapper for amqp.Delivery which implements the
// occamy.Message interface.
type AMQPMessage struct {
	msg amqp.Delivery
}

// ConvertToAMQPMessage creates an AMQPMessage from an amqp.Delivery.
func ConvertToAMQPMessage(delivery amqp.Delivery) *AMQPMessage {
	return &AMQPMessage{msg: delivery}
}

func (m *AMQPMessage) Body() []byte {
	return m.msg.Body
}

func (m *AMQPMessage) Headers() occamy.Headers {
	headers := make(occamy.Headers)
	if len(m.msg.Headers) != 0 {
		for k, v := range m.msg.Headers {
			headers[k] = v
		}
	}

	return headers
}

func (m *AMQPMessage) Ack() error {
	return m.msg.Ack(false)
}

func (m *AMQPMessage) Reject(requeue bool) error {
	return m.msg.Reject(requeue)
}

// endregion

// region AMQP Server Wrapper

// AMQPServer is a wrapper for the occamy.Server which includes methods for
// and handling amqp.Delivery messages.
type AMQPServer struct {
	occamy *occamy.Server
}

func NewAMQPServer(config occamy.ServerConfig) *AMQPServer {
	return &AMQPServer{
		occamy: occamy.NewServer(config),
	}
}

func (server *AMQPServer) HandleControlMsg(msg amqp.Delivery) {
	server.occamy.HandleControlMsg(ConvertToAMQPMessage(msg))
}

func (server *AMQPServer) HandleRequestMsg(msg amqp.Delivery) {
	server.occamy.HandleRequestMsg(ConvertToAMQPMessage(msg))
}

func (server *AMQPServer) Shutdown(ctx context.Context) {
	server.occamy.Shutdown(ctx)
}

// endregion

// region Connection

// createConnection creates an AMQP connection.
func createConnection(url string) *amqp.Connection {
	connection, err := amqp.Dial(url)
	if err != nil {
		log.Fatalf("unable to start connection: %v", err)
	}

	log.Println("connection establish")
	return connection
}

// endregion

// region Consume Control Messages

// consumeControlMessages declares a queue for control messages. A consumer is
// connected to this queue and messages are passed to the occamy server.
func consumeControlMessages(server *AMQPServer, connection *amqp.Connection) {
	channel, err := connection.Channel()
	if err != nil {
		log.Fatalf("failed to create channel to control consumer: %v", err)
	}

	err = channel.Qos(100, 0, false)
	if err != nil {
		log.Fatalf("failed to set prefetch to control consumer: %v", err)
	}

	// *IMPORTANT*: It is important that every message goes to every running
	// instance of this application.  This means each application
	// should have its own EXCLUSIVE queue for control messages and that these
	// can be removed once the application stops. It is for this reason the
	// queue name is random (and hopefully unique) for each application.
	name := fmt.Sprintf("%s_%05d", ControlExchange, rand.Intn(10000))
	const (
		autoDelete = true
		durable    = false
		exclusive  = true
	)

	queue, err := channel.QueueDeclare(name, durable, autoDelete, exclusive, false, nil)
	if err != nil {
		log.Fatalf("failed to declare queue for control consumer: %v", err)
	}

	err = channel.QueueBind(queue.Name, "#", ControlExchange, false, nil)
	if err != nil {
		log.Fatalf("failed to bind queue for control consumer: %v", err)
	}

	msgs, err := channel.Consume(queue.Name, "", false, exclusive, false, false, nil)
	if err != nil {
		log.Fatalf("failed to start consuming queue for control consumer: %v", err)
	}

	log.Println("consuming control messages")
	for {
		msg, ok := <-msgs
		if !ok {
			log.Fatal("no more messages in control consumer - this was expected to go forever")
		}

		server.HandleControlMsg(msg)
	}
}

// endregion

// region Consume Request Messages

// consumeRequestMessages declares a queue for request messages. A consumer is
// connected to this queue and messages are passed to the occamy server.
func consumeRequestMessages(server *AMQPServer, connection *amqp.Connection, slots int) {
	channel, err := connection.Channel()
	if err != nil {
		log.Fatalf("failed to create channel to request consumer: %v", err)
	}

	// The prefetch should be equal to the number of slots. If the prefetch is
	// higher the message will be instantly rejected and requeued, while if it
	// is lower the messages will be unnecessarily waiting.
	err = channel.Qos(slots, 0, false)
	if err != nil {
		log.Fatalf("failed to set prefetch to request consumer: %v", err)
	}

	// *IMPORTANT*: It is important that every messages goes to precisely one
	// running instance of this application (unless it gets requeued). To share
	// a queue the queue names just need to match. The queue must not be
	// exclusive.
	name := RequestExchange
	const (
		autoDelete = false
		durable    = true
		exclusive  = false
	)

	queue, err := channel.QueueDeclare(name, durable, autoDelete, exclusive, false, nil)
	if err != nil {
		log.Fatalf("failed to declare queue for request consumer: %v", err)
	}

	err = channel.QueueBind(queue.Name, "#", RequestExchange, false, nil)
	if err != nil {
		log.Fatalf("failed to bind queue for control consumer: %v", err)
	}

	msgs, err := channel.Consume(queue.Name, "", false, exclusive, false, false, nil)
	if err != nil {
		log.Fatalf("failed to start consuming queue for request consumer: %v", err)
	}

	log.Println("consuming request messages")
	for {
		msg, ok := <-msgs
		if !ok {
			log.Fatal("no more messages in control consumer - this was expected to go forever")
		}

		server.HandleRequestMsg(msg)
	}

}

// endregion

// region Exchange Declare

// declareExchanges declares the exchanges necessary for communication to the
// application.
func declareExchanges(connection *amqp.Connection, exchanges ...string) {
	channel, err := connection.Channel()
	if err != nil {
		log.Fatalf("unable to create channel to declare exchanges: %v", err)
	}

	// The queues exchanges should persist, so they can be used after restarts
	// of the applications.
	const (
		durable    = true
		autoDelete = false
		internal   = false
		noWait     = false
	)

	for _, exchange := range exchanges {
		err = channel.ExchangeDeclare(exchange, amqp.ExchangeTopic, durable, autoDelete, internal, noWait, nil)
		if err != nil {
			log.Fatalf("unable to declare exchange %s: %v", exchange, err)
		}
	}

	log.Println("exchanges declared")
}

// endregion

// region Handle

// handle is a stand in for your own handler.
func handle(_ occamy.Headers, body []byte) (occamy.Task, error) {
	// Implement your own task and handle logic!
	log.Printf("unable to handle message with body: %s\n", string(body))
	return nil, fmt.Errorf("handle function not implemented")
}

// endregion
