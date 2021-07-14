package io.pravega.connector.runtime.source;

import io.pravega.connector.runtime.PravegaWriter;
import io.pravega.connector.runtime.Task;
import io.pravega.connector.runtime.WorkerState;

import java.util.List;
import java.util.Map;

public class SourceTask extends Task {
    private Source source;
    private PravegaWriter pravegaWriter;
    private Map<String, String> pravegaProps;
    private volatile WorkerState workerState;
    private boolean stopping;
    public static String ROUTING_KEY_CONFIG = "routingKey";

    public SourceTask(PravegaWriter pravegaWriter, Source source, Map<String, String> pravegaProps, WorkerState workerState) {
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
            while (true) {
                if (hasPaused()) {
                    System.out.println(Thread.currentThread().getName() + " has paused");
                    awaitResume();
                    continue;
                }
                records = source.read();
                System.out.println(Thread.currentThread().getName() + " sourceRecord sizes: " + records.size());
                if (records.size() == 0) continue;
                for (int i = 0; i < records.size(); i++)
                    sendRecord(records.get(i));
            }
        } catch (InterruptedException e) {
            e.printStackTrace();
        } finally {
            source.close();
            pravegaWriter.close();

        }

    }


    public void sendRecord(SourceRecord record) {
        pravegaWriter.run(pravegaProps.get(ROUTING_KEY_CONFIG), record.getValue());
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


}
