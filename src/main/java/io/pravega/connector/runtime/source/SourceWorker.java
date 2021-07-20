package io.pravega.connector.runtime.source;

import io.pravega.client.admin.StreamManager;
import io.pravega.client.stream.ScalingPolicy;
import io.pravega.client.stream.StreamConfiguration;
import io.pravega.connector.runtime.*;

import java.net.URI;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.LinkedBlockingDeque;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

public class SourceWorker implements Workers {
    private final ExecutorService executor;
    private Map<String, String> pravegaProps;
    private Map<String, List<Task>> tasks;
    private volatile WorkerState workerState;
    public static String SOURCE_CLASS_CONFIG = "class";
    public static String CONNECT_NAME_CONFIG = "name";
    public static String TASK_NUM_CONFIG = "tasks.max";
    public static String SCOPE_CONFIG = "scope";
    public static String STREAM_NAME_CONFIG = "streamName";
    public static String URI_CONFIG = "uri";
    public static String SERIALIZER_CONFIG = "serializer";
    public static String SEGMENTS_NUM_CONFIG = "segments";
    public static String TRANSACTION_ENABLE_CONFIG = "transaction.enable";



    public SourceWorker(Map<String, String> pravegaProps, Map<String, String> sourceProps, WorkerState workerState) {
        this.executor = new ThreadPoolExecutor(20, 200, 60L, TimeUnit.SECONDS, new LinkedBlockingDeque<Runnable>());
        this.pravegaProps = pravegaProps;
        this.workerState = workerState;
        this.tasks = new HashMap<>();
    }

    public void startConnector(Map<String, String> connectorProps) {
        try {
            Class<?> sourceClass = null;
            int threadNum = Integer.valueOf(connectorProps.get(TASK_NUM_CONFIG));
            List<Source> sourceGroup = new ArrayList<>();
            initializePravega(pravegaProps);
            for (int i = 0; i < threadNum; i++) {
                sourceClass = Class.forName(connectorProps.get(SOURCE_CLASS_CONFIG));
                Source source = (Source) sourceClass.newInstance();
                source.open(connectorProps, pravegaProps);
                sourceGroup.add(source);
            }
            for (int i = 0; i < threadNum; i++) {
                Writer writer = null;
                if(connectorProps.containsKey(TRANSACTION_ENABLE_CONFIG) && connectorProps.get(TRANSACTION_ENABLE_CONFIG).equals("true")) {
                    writer = new PravegaTransactionalWriter(pravegaProps);
                }
                else writer = new PravegaWriter(pravegaProps);
                writer.initialize();
                SourceTask sourceTask = new SourceTask(writer, sourceGroup.get(i), pravegaProps, WorkerState.Started);
                tasks.putIfAbsent(connectorProps.get(CONNECT_NAME_CONFIG), new ArrayList<>());
                tasks.get(connectorProps.get(CONNECT_NAME_CONFIG)).add(sourceTask);
                executor.submit(sourceTask);
            }
//        executor.shutdown();
        } catch (Exception e) {
            e.printStackTrace();
        }


    }

    public void setWorkerState(WorkerState workerState, String workerName) {
        this.workerState = workerState;
        List<Task> tasksList = tasks.get(workerName);
        if (tasksList == null) return;
        for (Task task : tasksList) {
            task.setState(workerState);
        }
    }

    public boolean initializePravega(Map<String, String> pravegaProps) throws ClassNotFoundException {
        String scope = pravegaProps.get(SCOPE_CONFIG);
        String streamName = pravegaProps.get(STREAM_NAME_CONFIG);
        URI controllerURI = URI.create(pravegaProps.get(URI_CONFIG));
        Class serializerClass = Class.forName(pravegaProps.get(SERIALIZER_CONFIG));
        StreamManager streamManager = StreamManager.create(controllerURI);
        final boolean scopeIsNew = streamManager.createScope(scope);

        StreamConfiguration streamConfig = StreamConfiguration.builder()
                .scalingPolicy(ScalingPolicy.fixed(Integer.valueOf(pravegaProps.get(SEGMENTS_NUM_CONFIG))))
                .build();
        final boolean streamIsNew = streamManager.createStream(scope, streamName, streamConfig);
        streamManager.close();
        return true;

    }

    @Override
    public void shutdownScheduledService() {

    }

    @Override
    public void deleteTasksConfig(String connectorName) {
        if (tasks.containsKey(connectorName)) tasks.remove(connectorName);
        return;
    }

    @Override
    public void deleteConnectorConfig(String connectorName) {
//        if (connectors.containsKey(connectorName)) connectors.remove(connectorName);
        return;
    }

    @Override
    public synchronized void stopConnector(String connectorName) {
        setWorkerState(WorkerState.Stopped, connectorName);
        deleteTasksConfig(connectorName);
        deleteConnectorConfig(connectorName);
    }

    @Override
    public Map<String, String> getConnectorConfig(String connectorName) {
        return null;
    }

}
