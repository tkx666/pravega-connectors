package io.pravega.connecter.file.source;

import io.pravega.connecter.utils.Utils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;
import java.util.Properties;

public class ConnectStandalone {
    private static final Logger log = LoggerFactory.getLogger(ConnectStandalone.class);

    public static void main(String[] args) {
        log.info("start pravega connect standalone");
        Properties pravegaProps = Utils.loadProps("pravega.properties");
        Properties fileProps = Utils.loadProps("file.properties");
        Map<String, String> pravegaMap = Utils.propsToMap(pravegaProps);
        Map<String, String> fileMap = Utils.propsToMap(fileProps);

        PravegaWriter.init(pravegaMap);
        PravegaWriter pravegaWriter = new PravegaWriter(pravegaMap);
        FileSource fileSource = new FileSource();
        fileSource.open(fileMap, pravegaMap);

        WorkerFileSource workerFileSource= new WorkerFileSource(pravegaWriter, fileSource, pravegaMap);
        Thread worker = new Thread(workerFileSource);
        worker.start();
    }

}
