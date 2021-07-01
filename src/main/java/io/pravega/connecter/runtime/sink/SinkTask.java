package io.pravega.connecter.runtime.sink;

import io.pravega.client.stream.EventRead;
import io.pravega.connecter.runtime.PravegaReader;
import io.pravega.connecter.runtime.Task;
import io.pravega.connecter.runtime.WorkerState;

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

    public boolean isStopped() {
        return this.workerState == WorkerState.Stopped;
    }
}
