package io.pravega.connector.runtime.source;

import io.pravega.connector.runtime.PravegaWriter;
import io.pravega.connector.runtime.Task;
import io.pravega.connector.runtime.WorkerState;
import io.pravega.connector.runtime.Writer;

import java.util.List;
import java.util.Map;

public class SourceTask extends Task {
    private Source source;
    private Writer pravegaWriter;
    private Map<String, String> pravegaProps;
    private volatile WorkerState workerState;
    private boolean stopping;
    public static String ROUTING_KEY_CONFIG = "routingKey";

    public SourceTask(Writer pravegaWriter, Source source, Map<String, String> pravegaProps, WorkerState workerState) {
        this.source = source;
        this.pravegaProps = pravegaProps;
        this.pravegaWriter = pravegaWriter;
        this.workerState = workerState;
        this.stopping = false;
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
        return workerState == WorkerState.Paused;
    }

    public boolean awaitResume() throws InterruptedException {
        synchronized (this) {
            while (workerState == WorkerState.Paused) {
                this.wait();
            }
            return true;
        }

    }
    @Override
    public void setState(WorkerState state) {
        synchronized (this) {
            if (workerState == WorkerState.Stopped)
                return;
            this.workerState = state;
            this.notifyAll();
        }
    }

    public boolean isStopped() {
        return this.workerState == WorkerState.Stopped;
    }
}
