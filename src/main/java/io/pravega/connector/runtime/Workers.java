package io.pravega.connector.runtime;

import io.pravega.connector.runtime.sink.SinkWorker;
import io.pravega.connector.runtime.source.SourceWorker;

import java.util.Map;

public interface Workers {
    void startConnector(Map<String, String> connectorProps);

    void setWorkerState(WorkerState workerState, String workerName);

    void shutdownScheduledService();
    void deleteTasksConfig(String connectorName);
    void deleteConnectorConfig(String connectorName);
    void stopConnector(String connectorName);
    Map<String, String> getConnectorConfig(String connectorName);


    static Workers getWorkerByType(Map<String, String> connectorProps, Map<String, String> pravegapProps) {
        if (connectorProps.get("type").equals("source")) {
            return new SourceWorker(pravegapProps, connectorProps, WorkerState.Started);
        } else if (connectorProps.get("type").equals("sink")) {
            return new SinkWorker(pravegapProps, connectorProps);
        }
        return null;
    }
}
