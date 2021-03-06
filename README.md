# Occamy

<p align="center">
<em>
Servers that utilise spare resources for computational intense distributed tasks.
</em>
</p>

Elastic horizontal scaling of services in cloud infrastructure is a convenient and effective way to provide the required
resources for running a service. For computational intense request which have irregular and unpredictable schedules
resources often end up being overprovisioned/underutilised to ensure that requests can be handled promptly.

This library contains a server that utilises spare provisioned resources. The server

1. ensures resources can promptly handle any incoming request, and
2. uses spare resources to assist in processing current requests.

The core idea is to have computational tasks which expand by creating assistant tasks to help the computation and can be
stopped to provide resources for new incoming request with the server providing coordination.

#### Documentation

- The [concept document](./documentation/concept.pdf) contains the concept for an Occamy server. It is written
  independent of implementation and is a useful starting point to understanding the core design.

- The [implemention document](./documentation/implementation.md) contains comments on  the implementation of the Occamy
  server as well as useful advice for using this library and implementing the required interfaces.

- The [examples module](./examples) contains example implementations of interfaces appearing in this module as well as
  some example use cases. This provides a practical demonstration on how different aspects of occamy work.

  It is recommended that new users start by copying the relevant code from the examples as a way to quickly get started.

#### Maintainer

- [Peter Finch](github.com/PeterEFinch)