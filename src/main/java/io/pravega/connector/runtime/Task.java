package io.pravega.connector.runtime;

public abstract class Task implements Runnable{

    @Override
    public void run() {
        execute();
    }

    /**
     * execute the task
     */
    protected abstract void execute();

    /**
     * Set the state of the task
     * @param state
     */
    public abstract void setState(ConnectorState state);

    /**
     * initialize the task
     */
    public abstract void initialize();
}
