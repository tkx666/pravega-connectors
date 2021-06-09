package io.pravega.connecter.runtime;

import io.pravega.connecter.runtime.sink.SinkWorker;
import io.pravega.connecter.runtime.source.SourceWorker;

import java.util.Map;

public interface Worker {
    public void execute(int nThread);
    static Worker getWorkerByType(Map<String, String> connectorProps, Map<String, String> pravegapProps){
        if(connectorProps.get("type").equals("source")){
            return new SourceWorker(pravegapProps, connectorProps);
        }
        else{
            return new SinkWorker(pravegapProps, connectorProps);
        }
    }
}
