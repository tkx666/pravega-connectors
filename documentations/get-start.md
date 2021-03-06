# Getting Started with Pravega Connect

## Prerequisites
To complete this guide, you need:

* JDK 8 or 11 installed with JAVA_HOME configured appropriately
* Pravega running(Check [here](https://pravega.io/docs/latest/getting-started/) to get started with Pravega)
* Use Gradle

## Basic Concept
click [here](https://github.com/tkx666/pravega-connectors/blob/main/documentations/concept.md) to see the basic concept of the Pravega Connect framework

## Start Pravega Connect 
The following command line can start the worker and connector tasks
```
bin\pravega-connectors -worker worker.properties -connector connector.properties
```

The first parameter is the worker's configuration which contains the pravega and RESTful server configuration. Note that the parameter is a file path. You can use any valid file path for the configuration. You can see the details [here](#worker-configuration)


The second parameter is the connector task's configuration. You can see the details [here](#source-configuration)

## Example
We provide the [worker configuration example](https://github.com/tkx666/pravega-connectors/blob/main/worker.properties)

We provide the [Kafka Connector](https://github.com/tkx666/pravega-connectors/tree/main/src/main/java/io/pravega/connector/kafka) with its source [configurations](https://github.com/tkx666/pravega-connectors/blob/main/kafkaSource.properties) and sink [configuration](https://github.com/tkx666/pravega-connectors/blob/main/kafkaSink.properties).

There is also a simple [File Connector](https://github.com/tkx666/pravega-connectors/tree/main/src/main/java/io/pravega/connector/file) with its source [configuration](https://github.com/tkx666/pravega-connectors/blob/main/fileSource.properties) and sink [configuration](https://github.com/tkx666/pravega-connectors/blob/main/kafkaSink.properties).

## Develop Guide
click [here](https://github.com/tkx666/pravega-connectors/blob/main/documentations/develop-guide.md) to see how to develop a connector

## RESTful API
click [here](https://github.com/tkx666/pravega-connectors/blob/main/documentations/restful-api.md) to see the RESTful API to manage the worker and tasks.

## Worker Configuration
For the example of worker configuration, you can see

```https://github.com/tkx666/pravega-connectors/blob/main/pravega.properties```

### Basic Worker Configuration
`scope`

the scope of the pravega
* Type: String
* Default: null

`streamName`

the stream of the pravega
* Type: String
* Default: null

`uri`

the uri of the pravega
* Type: String
* Default: tcp://127.0.0.1:9090

`serializer`

the serializer of the pravega
* Type: String
* Default: io.pravega.client.stream.impl.UTF8StringSerializer

`segments`

the segments of the pravega
* Type: Integer
* Default: 5

`readerGroup`

the reader group of the pravega
* Type: String
* Default: null

`routingKey.class`

the class of generating routing key for pravega
* Type: String
* Default: io.pravega.connector.runtime.DefaultRoutingKeyGenerator

`rest.port`

the port of the RESTful server
* Type: Integer
* Default: 8091

## Source Configuration
For the example of worker configuration, you can see

```https://github.com/tkx666/pravega-connectors/blob/main/kafkaSource.properties```

The example contains the basic source configuration and custom configuration for kafka source.

Source task support transaction.

### Basic Source Configuration
`type`

type of the connector(sink or source)
* Type: String
* Default: null

`tasks.max`

the number of tasks for the connector
* Type: Integer
* Default: 1

`name`

the unique name of the connector
* Type: String
* Default: null

`class`

the class which implements the Source interface
* Type: String
* Default: null

`transaction.enable`

enable the transaction for Source task
* Type: String
* Default: false

## Sink Configuration
For the example of worker configuration, you can see

```https://github.com/tkx666/pravega-connectors/blob/main/kafkaSink.properties```

The example contains the basic sink configuration and custom configuration for kafka sink.

Sink task supports checkpoint for prevega.

### Basic Sink Configuration
`type`

type of the connector(sink or source)
* Type: String
* Default: null

`tasks.max`

the number of tasks for the connector
* Type: Integer
* Default: 1

`name`

the unique name of the connector
* Type: String
* Default: null

`class`

the class which implements the Source interface
* Type: String
* Default: null

`checkpoint.enable`

enable the checkpoint for Source task
* Type: String
* Default: true

`checkpoint.name`

the name of the checkpoint
* Type: String
* Default: null

`checkpoint.persist.path`

the file path for persisting checkpoint
* Type: String
* Default: null