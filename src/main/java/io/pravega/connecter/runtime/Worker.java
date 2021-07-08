package io.pravega.connecter.runtime;

import io.pravega.connecter.runtime.sink.SinkWorker;
import io.pravega.connecter.runtime.source.SourceWorker;

import java.util.List;
import java.util.Map;

public interface Worker {
    void startConnector(Map<String, String> sinkProps);

    void setWorkerState(WorkerState workerState, String workerName);

    void shutdownScheduledService();
    void deleteTasksConfig(String connectorName);
    void deleteConnectorConfig(String connectorName);
    Map<String, String> getConnectorConfig(String connectorName);


    static Worker getWorkerByType(Map<String, String> connectorProps, Map<String, String> pravegapProps) {
        if (connectorProps.get("type").equals("source")) {
            return new SourceWorker(pravegapProps, connectorProps, WorkerState.Started);
        } else if (connectorProps.get("type").equals("sink")) {
            return new SinkWorker(pravegapProps, connectorProps);
        }
        return null;
    }
}
