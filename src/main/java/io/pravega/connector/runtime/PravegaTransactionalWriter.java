package io.pravega.connector.runtime;

import io.pravega.client.ClientConfig;
import io.pravega.client.EventStreamClientFactory;
import io.pravega.client.stream.EventWriterConfig;
import io.pravega.client.stream.Serializer;
import io.pravega.client.stream.Transaction;
import io.pravega.client.stream.TransactionalEventStreamWriter;
import io.pravega.connector.runtime.configs.WorkerConfig;
import io.pravega.connector.runtime.source.SourceRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.URI;
import java.util.List;
import java.util.Map;
/**
 * PravegaTransactionalWriter encapsulates the TransactionalEventStreamWriter.
 */
public class PravegaTransactionalWriter implements Writer {
    private static final Logger logger = LoggerFactory.getLogger(PravegaTransactionalWriter.class);
    private TransactionalEventStreamWriter<Object> writer;
    private Map<String, String> pravegaProps;
    private EventStreamClientFactory clientFactory;

    public PravegaTransactionalWriter(Map<String, String> pravegaProps) {
        this.pravegaProps = pravegaProps;
    }

    public boolean initialize() {
        try {
            Class<?> serializerClass = Class.forName(pravegaProps.get(WorkerConfig.SERIALIZER_CONFIG));
            String scope = pravegaProps.get(WorkerConfig.SCOPE_CONFIG);
            URI controllerURI = URI.create(pravegaProps.get(WorkerConfig.URI_CONFIG));
            String streamName = pravegaProps.get(WorkerConfig.STREAM_NAME_CONFIG);
            clientFactory = EventStreamClientFactory.withScope(scope,
                    ClientConfig.builder().controllerURI(controllerURI).build());
            writer = clientFactory.createTransactionalEventWriter(streamName,
                    (Serializer) serializerClass.newInstance(),
                    EventWriterConfig.builder()
                            .transactionTimeoutTime(60000)
                            .build());
        } catch (Exception e) {
            logger.error("writer initialize error", e);
        }
        return true;

    }

    @Override
    public void write(List<SourceRecord> records) {
        Transaction<Object> transaction = writer.beginTxn();
        try {
            for (SourceRecord record : records) {
                Object message = record.getValue();
                transaction.writeEvent(message);
                transaction.flush();
            }
            transaction.commit();
        } catch (Exception e) {
            transaction.abort();
            logger.error("transaction abort", e);
        }
    }

    @Override
    public void close() {
        writer.close();
        clientFactory.close();
    }
}
