package io.pravega.connector.runtime;

public abstract class Task implements Runnable{

    @Override
    public void run() {
        execute();
    }

    protected abstract void execute();

    public abstract void setState(ConnectorState state);

    public abstract void initialize();
}
