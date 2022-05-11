package occamy

// Headers contain the header information
// of a message.
type Headers map[string]interface{}

// Message represents an asynchronous message.
type Message interface {
	// Body must return the body of the message.
	Body() []byte

	// Headers must return the headers of the message.
	Headers() Headers

	// Ack must acknowledge the messages as successfully handled and does not
	// need to be passed to another server.
	Ack() error

	// Reject must negative acknowledge the message as uncompleted. The requeue
	// parameter will determine if the message should be requeued and passed to
	// another server.
	//
	// For example, if a server shuts down the task associated with a message
	// will not be completed and will be passed to another server. On the other
	// hand if
	Reject(requeue bool) error
}
