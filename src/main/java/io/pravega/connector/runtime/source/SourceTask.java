package io.pravega.connector.runtime.source;

import io.pravega.connector.runtime.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.Map;

public class SourceTask extends Task {
    private static final Logger logger = LoggerFactory.getLogger(SourceTask.class);
    private Source source;
    private Writer pravegaWriter;
    private Map<String, String> pravegaProps;
    private Map<String, String> sourceProps;
    private int id;
    private volatile ConnectorState connectorState;
    private boolean stopping;

    public SourceTask(SourceConfig sourceConfig, WorkerConfig workerConfig, ConnectorState connectorState, int id) {
        this.pravegaProps = workerConfig.getStringConfig();
        this.connectorState = connectorState;
        this.stopping = false;
        this.sourceProps = sourceConfig.getStringConfig();
        this.id = id;
    }


    @Override
    public void initialize() {
        try {
            if (sourceProps.containsKey(SourceConfig.TRANSACTION_ENABLE_CONFIG) && sourceProps.get(SourceConfig.TRANSACTION_ENABLE_CONFIG).equals("true")) {
                pravegaWriter = new PravegaTransactionalWriter(pravegaProps);
            } else
                pravegaWriter = new PravegaWriter(pravegaProps);
            pravegaWriter.initialize();
            Class sourceClass = Class.forName(sourceProps.get(ConnectorConfig.CLASS_CONFIG));
            this.source = (Source) sourceClass.newInstance();
//            source.config().validate(sourceProps);
            source.open(sourceProps);
        } catch (Exception e) {
            logger.error("source task initialize error", e);
        }

    }

    @Override
    protected void execute() {
        try {
            List<SourceRecord> records;
            while (!isStopped()) {
                if (hasPaused()) {
                    logger.info(Thread.currentThread().getName() + " source task has paused");
                    awaitResume();
                    continue;
                }
                records = source.read();
                logger.info(Thread.currentThread().getName() + " sourceRecord size: " + records.size());
                if (records.size() == 0) continue;
                sendRecord(records);

            }
        } catch (InterruptedException e) {
            logger.error("source task execute error", e);
        } finally {
            source.close();
            pravegaWriter.close();

        }

    }

    public void sendRecord(List<SourceRecord> records) {
        try {
            pravegaWriter.write(records);
        } catch (Exception e) {
            logger.error("write to pravega fail", e);
        }
    }

    public boolean hasPaused() {
        return connectorState == ConnectorState.Paused;
    }

    public boolean awaitResume() throws InterruptedException {
        synchronized (this) {
            while (connectorState == ConnectorState.Paused) {
                this.wait();
            }
            return true;
        }

    }
    @Override
    public void setState(ConnectorState state) {
        synchronized (this) {
            if (connectorState == ConnectorState.Stopped)
                return;
            this.connectorState = state;
            this.notifyAll();
        }
    }



    public boolean isStopped() {
        return this.connectorState == ConnectorState.Stopped;
    }
}
