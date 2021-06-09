package io.pravega.connecter.runtime.cli;

import io.pravega.connecter.runtime.sink.SinkWorker;
import io.pravega.connecter.utils.Utils;
import org.apache.commons.cli.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;
import java.util.Properties;

public class ConnectSinkStandalone {    private static final Logger log = LoggerFactory.getLogger(ConnectSourceStandalone.class);

    public static void main(String[] args) {
        log.info("start pravega connect standalone");
        CommandLineParser commandParser = new DefaultParser();
        Options options = new Options();

        options.addOption("pravega", true, "properties of pravega");
        options.addOption("connector", true, "properties of connector");
        CommandLine cli = null;
        try {
            cli = commandParser.parse(options, args);
            String pravegaPath = cli.getOptionValue("pravega");
            Properties pravegaProps = Utils.loadProps(pravegaPath);
            String connectorPath = cli.getOptionValue("connector");
            Properties connectorProps = Utils.loadProps(connectorPath);
            Map<String, String> pravegaMap = Utils.propsToMap(pravegaProps);
            Map<String, String> connectorMap = Utils.propsToMap(connectorProps);
            SinkWorker sinkWorker = new SinkWorker(pravegaMap, connectorMap);
            sinkWorker.execute(Integer.valueOf(connectorMap.get("tasks.max")));


        } catch (Exception e) {
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
