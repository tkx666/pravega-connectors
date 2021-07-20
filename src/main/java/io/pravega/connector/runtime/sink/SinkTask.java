package io.pravega.connector.runtime.sink;

import io.pravega.connector.runtime.PravegaReader;
import io.pravega.connector.runtime.Task;
import io.pravega.connector.runtime.WorkerState;

import java.util.List;
import java.util.Map;

public class SinkTask extends Task {
    private Sink sink;
    private PravegaReader pravegaReader;
    private Map<String, String> pravegaProps;
    private WorkerState workerState;

    public SinkTask(PravegaReader pravegaReader, Sink sink, Map<String, String> pravegaProps, WorkerState state) {
        this.sink = sink;
        this.pravegaReader = pravegaReader;
        this.pravegaProps = pravegaProps;
        this.workerState = state;
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
                readList = pravegaReader.readEvent();
                System.out.println(Thread.currentThread() + "  size: " + readList.size());
                if (readList.size() == 0) continue;
                sink.write(readList);
            }
        } catch (Exception e){
            e.printStackTrace();
        }
        finally {
            sink.close();
            pravegaReader.close();
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

    public boolean isStopped() {
        return this.workerState == WorkerState.Stopped;
    }
}
