package io.pravega.connector.runtime.sink;

import io.pravega.connector.runtime.PravegaReader;
import io.pravega.connector.runtime.Task;
import io.pravega.connector.runtime.ConnectorState;

import java.util.List;
import java.util.Map;

public class SinkTask extends Task {
    private Sink sink;
    private PravegaReader reader;
    private Map<String, String> pravegaProps;
    private ConnectorState connectorState;
    private Map<String, String> sinkProps;
    private int id;

    public static String SINK_CLASS_CONFIG = "class";
    public static String SINK_NAME_CONFIG = "name";



    public SinkTask(Map<String, String> sinkProps, Map<String, String> pravegaProps, ConnectorState state, int id) {
        this.sinkProps = sinkProps;
        this.pravegaProps = pravegaProps;
        this.connectorState = state;
        this.id = id;
    }
    public SinkTask(PravegaReader reader, Sink sink, Map<String, String> pravegaProps, ConnectorState state, int id) {
//        this.sinkProps = sinkProps;
        this.pravegaProps = pravegaProps;
        this.connectorState = state;
        this.id = id;
        this.reader = reader;
        this.sink = sink;
    }

    @Override
    public void initialize() {
        try {
            Class sinkClass = Class.forName(sinkProps.get(SINK_CLASS_CONFIG));
            this.sink = (Sink) sinkClass.newInstance();
            sink.config().validate(sinkProps);
            sink.open(sinkProps);
            this.reader = new PravegaReader(pravegaProps, sinkProps.get(SINK_NAME_CONFIG) + id);
            reader.initialize(pravegaProps);
        } catch (Exception e) {
            e.printStackTrace();
        }

    }
    @Override
    protected void execute() {
        try {
            List<SinkRecord> readList = null;
            while (!isStopped()) {
                if (hasPaused()) {
                    System.out.println(Thread.currentThread().getName() + " has paused");
                    awaitResume();
                    continue;
                }
                readList = reader.readEvent();
                System.out.println(Thread.currentThread() + "  size: " + readList.size());
                if (readList.size() == 0) continue;
                sink.write(readList);
            }
        } catch (Exception e){
            e.printStackTrace();
        }
        finally {
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
