package io.pravega.connector.runtime.cli;

import io.pravega.connector.runtime.Worker;
import io.pravega.connector.runtime.configs.WorkerConfig;
import io.pravega.connector.runtime.rest.RestServer;
import io.pravega.connector.utils.Utils;
import org.apache.commons.cli.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;
import java.util.Properties;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

/**
 * Start the Pravega Connect process. The class parse the args from the command line and initialize the Worker and RESTful server
 */

public class ConnectStandalone {
    private static final Logger logger = LoggerFactory.getLogger(ConnectStandalone.class);
    public static String WORKER_OPTION_CONFIG = "worker";
    public static String CONNECTOR_OPTION_CONFIG = "connector";

    public static void main(String[] args) {
        logger.info("start pravega connect standalone");
        CommandLineParser commandParser = new DefaultParser();
        Options options = new Options();

        options.addOption(WORKER_OPTION_CONFIG, true, "properties of pravega");
        options.addOption(CONNECTOR_OPTION_CONFIG, true, "properties of connector");
        ExecutorService connectorsThreadPool = Executors.newCachedThreadPool();

        try {
            CommandLine cli = commandParser.parse(options, args);
            String pravegaPath = cli.getOptionValue(WORKER_OPTION_CONFIG);
            Properties pravegaProps = Utils.loadProps(pravegaPath);
            String connectorPath = cli.getOptionValue(CONNECTOR_OPTION_CONFIG);
            Properties connectorProps = Utils.loadProps(connectorPath);

            Map<String, String> pravegaMap = Utils.propsToMap(pravegaProps);
            Map<String, String> connectorMap = Utils.propsToMap(connectorProps);
            WorkerConfig workerConfig = new WorkerConfig(pravegaMap);
            Worker worker = new Worker(workerConfig);

            RestServer server = new RestServer(pravegaMap);
            server.initializeServer();
            server.initializeResource(worker);
            worker.addShutDownHook();
            connectorsThreadPool.submit(() -> { worker.startConnector(connectorMap); });

        } catch (Exception e) {
            logger.info("pravega connector running fail", e);
        }

    }


}

