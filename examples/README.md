# Examples

This module contains example implementations of interfaces appearing in
[occamy](./..) as well as some example use cases. The examples included are:

- An [AMQP server](./amqp_server/main.go): This example shows how to wrap around a message from amqp and implement the
  required message interface as well as how to create a wrapper for an occamy server which contains methods for handling
  message from amqp. There is also general points of discussion and important notes on requirements for queues and
  exchanges. 
