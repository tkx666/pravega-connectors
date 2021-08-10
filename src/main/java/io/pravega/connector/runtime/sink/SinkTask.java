package io.pravega.connector.runtime.sink;

import io.pravega.connector.runtime.*;
import io.pravega.connector.runtime.configs.ConnectorConfig;
import io.pravega.connector.runtime.configs.SinkConfig;
import io.pravega.connector.runtime.configs.WorkerConfig;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.Map;

/**
 * SinkTask that uses PravegaReader to read data from Pravega and uses Sink to write data to other system.
 */
public class SinkTask extends Task {
    private static final Logger logger = LoggerFactory.getLogger(SinkTask.class);
    private Sink sink;
    private PravegaReader reader;
    private Map<String, String> pravegaProps;
    private ConnectorState connectorState;
    private Map<String, String> sinkProps;
    private int id;

    public SinkTask(SinkConfig sinkConfig, WorkerConfig workerConfig, ConnectorState state, int id) {
        this.sinkProps = sinkConfig.getStringConfig();
        this.pravegaProps = workerConfig.getStringConfig();
        this.connectorState = state;
        this.id = id;
    }

    @Override
    public void initialize() {
        try {
            Class sinkClass = Class.forName(sinkProps.get(ConnectorConfig.CLASS_CONFIG));
            this.sink = (Sink) sinkClass.newInstance();
//            sink.config().validate(sinkProps);
            sink.open(sinkProps);
            this.reader = new PravegaReader(pravegaProps, sinkProps.get(ConnectorConfig.NAME_CONFIG) + id);
            reader.initialize(pravegaProps);
        } catch (Exception e) {
            logger.error("sink task initialize error", e);
        }

    }

    @Override
    protected void execute() {
        try {
            List<SinkRecord> readList = null;
            while (!isStopped()) {
                if (hasPaused()) {
                    logger.info(Thread.currentThread().getName() + " sink task has paused");
                    awaitResume();
                    continue;
                }
                readList = reader.readEvent();
                logger.info(Thread.currentThread() + "  sinkRecord size: " + readList.size());
                if (readList.size() == 0) continue;
                sink.write(readList);
            }
        } catch (Exception e) {
            logger.error("sink task running error", e);
        } finally {
            sink.close();
            reader.close();
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

    public boolean isStopped() {
        return this.connectorState == ConnectorState.Stopped;
    }
}
