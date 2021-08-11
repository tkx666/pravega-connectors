# Basic Concept

## Source
Source feeds data from external systems to Pravega

## Sink
Sink feeds data from Pravega to external systems

## Task
Task is a thread. Task's main responsibility is to stream data between Pravega and external system. A set of tasks can copy data parallelly by setting the configuration of a connector. 

## Worker
Worker create and manage tasks.

### Standalone Worker
Standalone Worker is a single process that manage all the tasks. You can update the state of the worker and task by RESTful API.