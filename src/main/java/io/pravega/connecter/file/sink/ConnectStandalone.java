package io.pravega.connecter.file.sink;

import io.pravega.connecter.utils.Utils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;
import java.util.Properties;

public class ConnectStandalone {    private static final Logger log = LoggerFactory.getLogger(io.pravega.connecter.file.source.ConnectStandalone.class);

    public static void main(String[] args) {
        log.info("start pravega connect standalone");
        Properties pravegaProps = Utils.loadProps("pravega.properties");
        Properties fileProps = Utils.loadProps("file.properties");
        Map<String, String> pravegaMap = Utils.propsToMap(pravegaProps);
        Map<String, String> fileMap = Utils.propsToMap(fileProps);
        SinkWorker sinkWorker = new SinkWorker(fileMap, pravegaMap);
        sinkWorker.execute(10);


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
