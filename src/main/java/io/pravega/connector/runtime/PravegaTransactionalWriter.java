package io.pravega.connector.runtime;

import com.sun.jndi.dns.ResourceRecord;
import io.pravega.client.ClientConfig;
import io.pravega.client.EventStreamClientFactory;
import io.pravega.client.stream.EventWriterConfig;
import io.pravega.client.stream.Serializer;
import io.pravega.client.stream.Transaction;
import io.pravega.client.stream.TransactionalEventStreamWriter;
import io.pravega.client.stream.impl.StreamImpl;
import io.pravega.client.stream.impl.TransactionalEventStreamWriterImpl;
import io.pravega.connector.runtime.source.SourceRecord;

import java.net.URI;
import java.util.List;
import java.util.Map;

public class PravegaTransactionalWriter implements Writer {
    private TransactionalEventStreamWriter<Object> writer;

    public static String SCOPE_CONFIG = "scope";
    public static String STREAM_NAME_CONFIG = "streamName";
    public static String URI_CONFIG = "uri";
    public static String SERIALIZER_CONFIG = "serializer";
    public static String ROUTING_KEY_CLASS_CONFIG = "routingKey.class";
    private Map<String, String> pravegaProps;

    public PravegaTransactionalWriter(Map<String, String> pravegaProps) {
        this.pravegaProps = pravegaProps;
    }

    public boolean initialize() {
        try {
            Class serializerClass = Class.forName(pravegaProps.get(SERIALIZER_CONFIG));
            String scope = pravegaProps.get(SCOPE_CONFIG);
            URI controllerURI = URI.create(pravegaProps.get(URI_CONFIG));
            String streamName = pravegaProps.get(STREAM_NAME_CONFIG);
            EventStreamClientFactory clientFactory = EventStreamClientFactory.withScope(scope,
                    ClientConfig.builder().controllerURI(controllerURI).build());
            writer = clientFactory.createTransactionalEventWriter(streamName,
                    (Serializer) serializerClass.newInstance(),
                    EventWriterConfig.builder()
                            .transactionTimeoutTime(60000)
                            .build());
        } catch (Exception e) {
            e.printStackTrace();
        }
        return true;

    }

    @Override
    public void write(List<SourceRecord> records) {
        Transaction<Object> transaction = writer.beginTxn();
        int count = 0;
        try {
            for (SourceRecord record : records) {
                Object message = record.getValue();
                transaction.writeEvent(message);
                transaction.flush();
                count++;
            }
            transaction.commit();
            System.out.println(transaction.checkStatus());
        } catch (Exception e) {
            e.printStackTrace();
            transaction.abort();
            System.out.println(transaction.checkStatus());
        }
    }

    @Override
    public void close() {
        writer.close();
    }
}
