package io.pravega.connecter.runtime.cli;

import io.pravega.connecter.runtime.sink.SinkWorker;
import io.pravega.connecter.utils.Utils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;
import java.util.Properties;

public class ConnectSinkStandalone {    private static final Logger log = LoggerFactory.getLogger(ConnectSourceStandalone.class);

    public static void main(String[] args) {
        log.info("start pravega connect standalone");
        Properties pravegaProps = Utils.loadProps("pravega.properties");
        Properties fileProps = Utils.loadProps("file.properties");
        Properties connectorProps = Utils.loadProps("sink.properties");

        Map<String, String> pravegaMap = Utils.propsToMap(pravegaProps);
        Map<String, String> fileMap = Utils.propsToMap(fileProps);
        Map<String, String> connectorMap = Utils.propsToMap(connectorProps);

        SinkWorker sinkWorker = new SinkWorker(fileMap, pravegaMap, connectorMap);
        try {
            sinkWorker.execute(Integer.valueOf(connectorMap.get("tasks.max")));
        } catch (ClassNotFoundException e) {
            e.printStackTrace();
        } catch (IllegalAccessException e) {
            e.printStackTrace();
        } catch (InstantiationException e) {
            e.printStackTrace();
        }

//        PravegaReader pravegaReader = new PravegaReader(pravegaMap);
//        FileSink fileSink = new FileSink();
//        fileSink.open(fileMap, pravegaMap);
//
//        FileSinkTask fileSinkTask = new FileSinkTask(pravegaReader, fileSink, pravegaMap);
//        Thread worker = new Thread(fileSinkTask);
//        worker.start();
//        //fileSink.close();
    }

}
