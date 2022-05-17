package main

import (
	"fmt"
	"sync"

	"github.com/Graphmasters/occamy"
)

const (
	HeaderKeyTaskID = "task-id"

	HeaderKeyHandlerID = "handler-id"
)

func main() {
	handler := NewHandlerMux(HeaderKeyHandlerID)
	// Register handlers here

	server := occamy.NewServer(occamy.ServerConfig{
		Slots:           8,
		HeaderKeyTaskID: HeaderKeyTaskID,
		Handler:         handler.Handle,
	})

	// Use server.HandleControlMsg & server.HandleRequestMsg to handle control
	// and request messages respectively.
	_ = server
}

type HandlerMux struct {
	headerKeyHandlerID string
	handlers           map[string]occamy.Handler
	mutex              *sync.Mutex
}

func NewHandlerMux(headerKeyHandlerID string) *HandlerMux {
	return &HandlerMux{
		headerKeyHandlerID: headerKeyHandlerID,
		handlers:           make(map[string]occamy.Handler),
		mutex:              &sync.Mutex{},
	}
}

func (h *HandlerMux) Handle(header occamy.Headers, body []byte) (occamy.Task, error) {
	if header == nil {
		return nil, fmt.Errorf("header contained no key value pairs")
	}

	headerValue, ok := header[h.headerKeyHandlerID]
	if !ok {
		return nil, fmt.Errorf("header is missing key-value pair to identify handler: require key %s", h.headerKeyHandlerID)
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

func (h *HandlerMux) Register(id string, handler occamy.Handler) error {
	h.mutex.Lock()
	defer h.mutex.Unlock()

	if _, ok := h.handlers[id]; ok {
		return fmt.Errorf("unable to register handler with id as the id (%s) has already been registered", id)
	}

	h.handlers[id] = handler
	return nil
}
