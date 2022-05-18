package main

import (
	"fmt"
	"sync"

	"github.com/Graphmasters/occamy"
)

const (
	HeaderKeyTaskID = "task-id" // HeaderKeyTaskID is the header key used to determine the task for control messages.

	HeaderKeyHandlerID = "handler-id" // HeaderKeyHandlerID is the header key used to determine the handler for request messages.

	HandlerIDA = "handler_a" // HandlerIDA is the ID associated with HandleA.
	HandlerIDB = "handler_b" // HandlerIDB is the ID associated with HandleB.
)

// main initialise a HandlerMux and registered two stand-in handlers.
func main() {
	handler := NewHandlerMux(HeaderKeyHandlerID)
	handler.Register(HandlerIDA, HandleA)
	handler.Register(HandlerIDB, HandleB)

	server := occamy.NewServer(occamy.ServerConfig{
		Slots:           8,
		HeaderKeyTaskID: HeaderKeyTaskID,
		Handler:         handler.Handle,
	})

	// Use server.HandleControlMsg & server.HandleRequestMsg to handle control
	// and request messages respectively.
	_ = server
}

// HandlerMux is an occamy.Handler multiplexer.
// It matches the value of a configured header key-value pair with a list of
// registered IDs and calls the handler for that ID.
type HandlerMux struct {
	headerKey string                    // headerKey is key for the header key-value pair whose value must match the id of a registered handler
	handlers  map[string]occamy.Handler // handlers contain all registered handlers with the key being id of the handler
	mutex     *sync.Mutex               // mutex previous clashes when accessing the handlers
}

// NewHandlerMux returns a new HandlerMux.
// The headerKey is key for the header key-value pair whose value will be used
// to match it with a registered handler.
func NewHandlerMux(headerKey string) *HandlerMux {
	return &HandlerMux{
		headerKey: headerKey,
		handlers:  make(map[string]occamy.Handler),
		mutex:     &sync.Mutex{},
	}
}

// Handle uses the header to determine which of the registered handlers should
// be used to create the task. If no suitable handler can be found an error
// will be returned.
func (h *HandlerMux) Handle(header occamy.Headers, body []byte) (occamy.Task, error) {
	if header == nil {
		return nil, fmt.Errorf("header contained no key value pairs")
	}

	headerValue, ok := header[h.headerKey]
	if !ok {
		return nil, fmt.Errorf("header is missing key-value pair to identify handler: require key %s", h.headerKey)
	}

	handlerID, ok := headerValue.(string)
	if !ok {
		return nil, fmt.Errorf("header value for ")
	}

	h.mutex.Lock()
	handler, ok := h.handlers[handlerID]
	h.mutex.Unlock()
	if !ok {
		return nil, fmt.Errorf("handler id from header (%s) does not match any registered handlers", handlerID)
	}

	return handler(header, body)
}

// Register registers the handler for the given ID.
// If the ID has already been registered an error will be thrown.
func (h *HandlerMux) Register(id string, handler occamy.Handler) {
	h.mutex.Lock()
	defer h.mutex.Unlock()

	if _, ok := h.handlers[id]; ok {
		panic("handler mux: multiple registrations for " + id)
	}

	h.handlers[id] = handler
}

// HandleA is a stand-in implementation of occamy.Handler.
func HandleA(header occamy.Headers, body []byte) (occamy.Task, error) {
	panic("implement me")
}

// HandleB is a stand-in implementation of occamy.Handler.
func HandleB(header occamy.Headers, body []byte) (occamy.Task, error) {
	panic("implement me")
}
