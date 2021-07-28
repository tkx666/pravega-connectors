package io.pravega.connector.runtime.source;

import io.pravega.connector.runtime.*;

import java.util.List;
import java.util.Map;

public class SourceTask extends Task {
    private Source source;
    private Writer pravegaWriter;
    private Map<String, String> pravegaProps;
    private Map<String, String> sourceProps;
    private int id;

    private volatile ConnectorState connectorState;
    private boolean stopping;
    public static String ROUTING_KEY_CONFIG = "routingKey";
    public static String TRANSACTION_ENABLE_CONFIG = "transaction.enable";
    public static String SOURCE_CLASS_CONFIG = "class";



    public SourceTask(Map<String, String> sourceProps, Map<String, String> pravegaProps, ConnectorState connectorState, int id) {
        this.pravegaProps = pravegaProps;
        this.connectorState = connectorState;
        this.stopping = false;
        this.sourceProps = sourceProps;
        this.id = id;
    }


    @Override
    public void initialize() {
        try {
            if (sourceProps.containsKey(TRANSACTION_ENABLE_CONFIG) && sourceProps.get(TRANSACTION_ENABLE_CONFIG).equals("true")) {
                pravegaWriter = new PravegaTransactionalWriter(pravegaProps);
            } else
                pravegaWriter = new PravegaWriter(pravegaProps);
            pravegaWriter.initialize();
            Class sourceClass = Class.forName(sourceProps.get(SOURCE_CLASS_CONFIG));
            this.source = (Source) sourceClass.newInstance();
            source.config().validate(sourceProps);
            source.open(sourceProps);
        } catch (Exception e) {
            e.printStackTrace();
        }

    }

    @Override
    protected void execute() {
        try {
            List<SourceRecord> records;
            while (!isStopped()) {
                if (hasPaused()) {
                    System.out.println(Thread.currentThread().getName() + " has paused");
                    awaitResume();
                    continue;
                }
                records = source.read();
                System.out.println(Thread.currentThread().getName() + " sourceRecord sizes: " + records.size());
                if (records.size() == 0) continue;
                sendRecord(records);

            }
        } catch (InterruptedException e) {
            e.printStackTrace();
        } finally {
            source.close();
            pravegaWriter.close();

        }

    }

    public void sendRecord(List<SourceRecord> records) {
        try {
            pravegaWriter.write(records);
        } catch (Exception e) {
            e.printStackTrace();
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
