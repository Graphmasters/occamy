# Occamy

Occamy is a library designed to be a central entry point for handling
computationally intense requests. It is intended designed to assist in the 
coordination of communication and allocation of resources for these requests.

The core constraints for the occamy server are:

- Requests are handled asynchronously but must be performed immediately.
- Computations are allowed to be done in a distributed fashion across multiple "machines"/"instances".
- On each instance there is a limited number of requests that can be handled concurrently. This is to avoid having a high CPU load on a server that degrades the results of request.
- Spare resources should be used whenever possible to avoid wasted resources but must be immediately freed up if required for a new incoming request.

### The Design

The general design and attributes of the server is as follows (for specific details see implementation):

- The occamy server is designed to be part of a distributed system. Each server should be independent and have zero knowledge of what the other servers are doing.

- The occamy server is independent of all logic regarding the actual handling of requests. This was chosen to separate the concerns of managing the CPU allotments for requests and performing the requests. The idea is that this package could be extracted into its own repository and used with other similar services.

- The occamy server is intended to be independent of messaging/queuing protocol even though it is inspired by RabbitMQ, which means there are some concepts that have been adopted. This was chosen to make changing to other technologies easier while not prematurely or unnecessarily abandoning RabbitMQ features.

- The occamy server is designed to handle asychronous messages. These messages should have a header and a body as well as being able to declared successfully completed or not, i.e. acked and nacked, respectively.
 
- The server is initialised with a given number of slots as well as a *handler* which can convert a body and header into a task. A task represents a computation that to be done and each slot may contain at most one task. This is to ensure that only a limited number of requests can be handled.

<p align="center">
<img src="images/slots.png" alt="communication" class="center" width="40%" height="40%">
</p>

- The server has two handle methods for two different types of messages. 
  - *Request messages* are messages that represent a request for computation. Requests messages are only sent to one occamy server. Each request will be converted into a task and should have an ID (which isn't necessarily unique). This task will be assigned to a slot.
  - *Control messages* are messages that provide information to running tasks or the occamy server itself. Control messages are passed to all occamy servers.
    Once a control message reaches a server it is passed on to one or more tasks based on the task ID in the header or just to the server. 
    External services and tasks are expected to communicate to tasks via control messages. For example, a request may be cancelled via a control message.
    If a control message is given without an associated task ID then the message is interpreted like a request messages and converted into a task using the handler, however, it is not immediately assigned to a slot but instead it is set aside to be used in the expansion process (see below).

<p align="center">
<img src="images/communication.png" alt="communication" class="center" width="30%" height="30%">
</p>
 
- The slots will have a status depending on the task.
  - *Empty* slots are not running and can be overwritten at any time.
  - Slots with *protected* tasks are running task which have been created due to an incoming request i.e. via the request handler.
    These tasks are never interrupted except when shutting the server down, in which the original requests will be rejected and requeued.
  - Slots with *unprotected* or *external* tasks are running tasks which have been generated to utilise spare resources and created in the expansion process (see below). 
    These can be interrupted at any time. Tasks must be implemented to accept being killed quickly and without consequence.

<p align="center">
<img src="images/slots_w_tasks.png" alt="slots with statuses" class="center" width="40%" height="40%">
</p>    

- At regular intervals the server will run an *expansion* process. 
  This is inspired by the fictional *occamy* creature from *Fantastic Beasts and Where to Find Them* in which the creature grew or shrank in order to fit available space.
  During this process the server will check if there is empty slots and attempt to generate new tasks in the following order:
  1. The protected tasks will be checked to see if they can be expanded (tasks are required to have an *Expand*) to create additional tasks. These tasks are given the status *unprotected*. It is important to note that expansion is optional.
  2. This is repeated for *unprotected* and *external* tasks, which will create tasks with the statuses *unprotected* and *external* respectively.
  3. The server will use any requests that came in as a control message. The task will be given the status *external*. 

<p align="center">
<img src="images/expansion.png" alt="expandsion" class="center" width="50%" height="50%">
</p> 

### Authors

- [Peter E. Finch](github.com/PeterEFinch)

<p align="center">
<img src="images/graphmasters_logo.svg" alt="logo" class="center" width="30%" height="30%">
</p> 