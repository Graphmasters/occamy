### Notes on Implementation

- The server has been implemented to be independent of messaging/queuing protocol. Consequently, a message has been defined as an interface which must be implemented. For example, the [AMQP server example](./../examples/amqp_server/main.go) contains a wrapper for an AMQP delivery message that  implements the required message interface.

- The server has been implemented to be independent of the requests being handled. There is an interface defined for tasks and a type (*Handler*) defined for converting the headers and body of request messages to tasks. The handler must be included when configuring the server.

- The server are initialised with the number of slots. It is recommended that each slots is associated with an equal amount of resources, i.e. CPU and memory. The general rule being that a slot is given the smallest CPU requirement for a single task, in the simplest case it is recommended to have one slot per CPU core. In the case that memory is a limiting factor, then choose a fewer number of slots and ensure that tasks can perform the computational intense process concurrently.

- If the messaging protocol has a prefetch limit then this should match the number of slots for the channel with the request messages. If prefetch limit is too low then it means that request messages that could be handled aren't. On the other hand, if the prefetch limit is too high then the service will regularly be rejecting messages because the server is full of *protected* tasks.``

- The server relies on headers key-value pairs to process messages, however, the header keys themselves are configurable. 
	- There must be a header key-value pair for task IDs. These are required for directing control messages to the relevant task. Control messages that do not have this header are treated as a request message and converted to a task and set aside for the expansion processes.

- The server will only behave as expected if the implementation of the handler and tasks is done correctly.
	- Converting requests to tasks should be fast otherwise this will likely block the processing of other requests.
	- Tasks must be resilient to being terminated as servers will regularly be shutdown. For *protected* tasks it is advised that progress is cached on a persistent layer and can be retrieved if the request is sent to a different server.
	- Tasks must be able to stop quickly	because *unprotected* tasks will be often be stopped to make way for protected tasks. 
	- Task must respect their resource allotments and not use excessive amounts of resources. If a slot is associated with a number of CPU cores then a task should have at most that many go routines performing the computational intense process. Tasks creating additional go routines for lightweight processes, like communication, is typically not an issue.

- For requests that require more CPU than associated with a single slot or would benefit from more resources it is recommended to use a *master/assistant* approach. The original request is converted into a *master* or *coordinator* task and should send out request message for assistant tasks to the occamy server. The number of assistants requested should be determined by considering the minimum required resources. Another request 
	
	Each task should be capable of running independently even if they are working on the same problem i.e. it shouldn't matter if two tasks are on the same machine or not. For example, memory should never be shared across tasks.

- The server does not contain any explicit monitoring or logging. Instead it includes interfaces for monitoring errors, latency and resource usage which have be set in the server config.


