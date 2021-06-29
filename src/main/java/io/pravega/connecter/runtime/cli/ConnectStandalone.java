package io.pravega.connecter.runtime.cli;

import io.pravega.connecter.runtime.Worker;
import io.pravega.connecter.runtime.rest.RestServer;
import io.pravega.connecter.runtime.sink.SinkWorker;
import io.pravega.connecter.runtime.source.SourceWorker;
import io.pravega.connecter.utils.Utils;
import org.apache.commons.cli.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;
import java.util.Properties;

public class ConnectStandalone {
    private static final Logger log = LoggerFactory.getLogger(ConnectStandalone.class);
    public static String PRAVEGA_OPTION_CONFIG = "pravega";
    public static String CONNECTOR_OPTION_CONFIG = "connector";
    public static String TASK_NUM_CONFIG = "tasks.max";


    public static void main(String[] args) {
        log.info("start pravega connect standalone");
        CommandLineParser commandParser = new DefaultParser();
        Options options = new Options();

        options.addOption(PRAVEGA_OPTION_CONFIG, true, "properties of pravega");
        options.addOption(CONNECTOR_OPTION_CONFIG, true, "properties of connector");

        try {
            CommandLine cli = commandParser.parse(options, args);
            String pravegaPath = cli.getOptionValue(PRAVEGA_OPTION_CONFIG);
            Properties pravegaProps = Utils.loadProps(pravegaPath);
            String connectorPath = cli.getOptionValue(CONNECTOR_OPTION_CONFIG);
            Properties connectorProps = Utils.loadProps(connectorPath);

            Map<String, String> pravegaMap = Utils.propsToMap(pravegaProps);
            Map<String, String> connectorMap = Utils.propsToMap(connectorProps);
            Worker worker = Worker.getWorkerByType(connectorMap, pravegaMap);

            RestServer server = new RestServer();
            server.initializeServer();
            server.initializeResource(worker);
            worker.execute(Integer.valueOf(connectorMap.get(TASK_NUM_CONFIG)));

        } catch (Exception e) {
            e.printStackTrace();
        }

    }


}

