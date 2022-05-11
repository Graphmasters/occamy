This document contains comments about the implementation including conventions etc.

### Comments on Implementation:

Complete me.

### Advice on Usage:

- Each task should be capable of running independently i.e. it shouldn't matter if two tasks are on the same machine or
  not. For example, memory should never be shared across tasks. Even if multiple tasks are working on the same problem
  the tasks should not care if the tasks are being done on the same instance or not.
- Tasks must be resilient to being terminated as occamy servers will regularly be stopped.
- Choosing the number of slots is important for ensuring efficient use of CPU and memory. It is advised to associate
  each slot with an amount of CPU. The general rule being that a slot is given the smallest CPU requirement for a single
  task, in the simplest case it is recommended to have one slot per CPU core. In the case that memory is a limiting
  factor, then choose a fewer number of slots and ensure that tasks can perform concurrent calculations.
- A task should respect the amount of CPU it is allowed. This is the responsibility of the handler that converts request
  messages into tasks.
- A request should determine how many tasks it needs by considering the original request and then sending additional
  request messages to the occamy server. This can be done using a master/assistant approach.
- A request should send a request for assistant tasks as a control message to the occamy server to be able to spread to
  all servers.
- If the messaging protocol has a prefetch limit then this should match the number of slots for the channel with the
  request messages.
