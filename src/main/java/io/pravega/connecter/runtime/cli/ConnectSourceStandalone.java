package io.pravega.connecter.runtime.cli;

import io.pravega.connecter.file.source.FileSource;
import io.pravega.connecter.runtime.PravegaWriter;
import io.pravega.connecter.runtime.source.SourceTask;
import io.pravega.connecter.runtime.source.SourceWorker;
import io.pravega.connecter.utils.Utils;
import org.apache.commons.cli.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;
import java.util.Properties;

public class ConnectSourceStandalone {
    private static final Logger log = LoggerFactory.getLogger(ConnectSourceStandalone.class);

    public static void main(String[] args) {
        log.info("start pravega connect standalone");
        CommandLineParser commandParser = new DefaultParser();
        Options options = new Options();

        options.addOption("pravega", true, "properties of pravega");
        options.addOption("file", true, "properties of file");
        options.addOption("connector", true, "properties of connector");

        try {
            CommandLine cli = commandParser.parse(options, args);
            String pravegaPath = cli.getOptionValue("pravega");
            Properties pravegaProps = Utils.loadProps(pravegaPath);
            String filePath = cli.getOptionValue("file");
            Properties fileProps = Utils.loadProps(filePath);
            String connectorPath = cli.getOptionValue("connector");
            Properties connectorProps = Utils.loadProps(connectorPath);

            Map<String, String> pravegaMap = Utils.propsToMap(pravegaProps);
            Map<String, String> fileMap = Utils.propsToMap(fileProps);
            Map<String, String> connectorMap = Utils.propsToMap(connectorProps);
            SourceWorker sourceWorker = new SourceWorker(fileMap, pravegaMap, connectorMap);
            sourceWorker.execute(Integer.valueOf(connectorMap.get("tasks.max")));

        } catch (Exception e) {
            e.printStackTrace();
        }


//        PravegaWriter.init(pravegaMap);
//
//        PravegaWriter pravegaWriter = new PravegaWriter(pravegaMap);
//        FileSource fileSource = new FileSource();
//        fileSource.open(fileMap, pravegaMap);
//
//        SourceTask sourceTask = new SourceTask(pravegaWriter, fileSource, pravegaMap);
//        Thread worker = new Thread(sourceTask);
//        worker.start();
    }

}
