package occamy

// Handle is a method that takes the header and body of a message and
// generates a task that must be completed. The header
// should contain relevant details about the task type
// and how to decode the message. The header will
// always contain cid in the field "X-Request-ID". The
// error returned should be one of the custom occamy
// errors.
type Handler func(header Headers, body []byte) (Task, error)
