package io.pravega.connector.runtime;

public abstract class Task implements Runnable{

    @Override
    public void run() {
        execute();
    }

    protected abstract void execute();

    public abstract void setState(WorkerState state);

    public abstract void initialize();
}
