#How to develop Pravega Connectors

This guide helps you to learn how to develop a Pravega connector to move data between Pravega and external systems.

|  Type   | Description  |
|  ----  | ----  |
| Source  | Import data from external system to Pravega |
| Sink  | Export data from Pravega to external system |

## Develop

You can develop the Sink connector and Source connector

### Source

You can implements the Source interface to develop the source connector

```java
public interface Source {
    /**
     * define the configuration for parse and validation
     *
     * @return the defined config
     */
    Config config();

    /**
     * initialize the sink
     *
     * @param sourceProps sink properties
     */
    void open(Map<String, String> sourceProps);

    /**
     * read the SourceRecord from other system
     *
     * @return a list of SourceRecord
     */
    List<SourceRecord> read();


    /**
     * close the sink task
     */
    void close();

}
```

1. implements config()

This method is used to parse and validate the configuration for the task. You can create a Config instance and add the expected configuration parameter to the instance. The methed should return the instance for the framework to validate. For example
```java
private static final Config config = new Config().add(SERVER_SERVERS_CONFIG, Config.Type.STRING, "localhost:9092", new Config.NonEmptyStringValidator())
        .add(KEY_DESERIALIZER_CONFIG, Config.Type.STRING, "org.apache.kafka.common.serialization.StringDeserializer", new Config.NonEmptyStringValidator())
        .add(VALUE_DESERIALIZER_CONFIG, Config.Type.STRING, "org.apache.kafka.common.serialization.StringDeserializer", new Config.NonEmptyStringValidator())
        .add(TOPIC_CONFIG, Config.Type.STRING, null, null)
        .add(GROUP_ID_CONFIG, Config.Type.STRING, null, null);

@Override
public Config config() {
    return config;
}
```

2. implements open(Map<String, String> sourceProps)

This method is called when the source connector is initialized. The method receives the config map which contains the configuration the connector requires to initialize. 

For example, a Kafka connector can create a Kafka consumer in this method.

3. implements read()

The method should return a list of SourceRecord. The list will be processed by the framework to send to the Pravega.

The SourceRecord contains a Object instance which is the value of the record.

