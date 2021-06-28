package io.pravega.connecter.runtime;

public abstract class Task implements Runnable{

    @Override
    public void run() {
        execute();
    }
    protected abstract void execute();
}
