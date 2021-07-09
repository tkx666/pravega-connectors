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

public class SourceWorker implements Worker {
    private final ExecutorService executor;
    private Map<String, String> pravegaProps;
//    private Map<String, String> sourceProps;
    private Map<String, List<Task>> tasks;
    private volatile WorkerState workerState;
    public static String SOURCE_CLASS_CONFIG = "class";
    public static String TASK_NUM_CONFIG = "tasks.max";
    public static String SCOPE_CONFIG = "scope";
    public static String STREAM_NAME_CONFIG = "streamName";
    public static String URI_CONFIG = "uri";
    public static String SERIALIZER_CONFIG = "serializer";
    public static String SEGMENTS_NUM_CONFIG = "segments";


    public SourceWorker(Map<String, String> pravegaProps, Map<String, String> sourceProps, WorkerState workerState) {
        this.executor = new ThreadPoolExecutor(20, 200, 60L, TimeUnit.SECONDS, new LinkedBlockingDeque<Runnable>());
        this.pravegaProps = pravegaProps;
//        this.sourceProps = sourceProps;
        this.workerState = workerState;
        this.tasks = new HashMap<>();
    }

    public void startConnector(Map<String, String> connectorProps) {
        try {
            Class<?> sourceClass = null;
            int threadNum = Integer.valueOf(connectorProps.get(TASK_NUM_CONFIG));
            List<Source> sourceGroup = new ArrayList<>();
            String workerName = connectorProps.get("name");
//            PravegaWriter.init(pravegaProps);
            initializePravega(pravegaProps);
            for (int i = 0; i < threadNum; i++) {
                sourceClass = Class.forName(connectorProps.get(SOURCE_CLASS_CONFIG));
                Source source = (Source) sourceClass.newInstance();
                source.open(connectorProps, pravegaProps);
                sourceGroup.add(source);
            }
            for (int i = 0; i < threadNum; i++) {
                PravegaWriter pravegaWriter = new PravegaWriter(pravegaProps);
                pravegaWriter.initialize(pravegaProps);
                SourceTask sourceTask = new SourceTask(pravegaWriter, sourceGroup.get(i), pravegaProps, WorkerState.Started);
                tasks.putIfAbsent(connectorProps.get("name"), new ArrayList<>());
                tasks.get(connectorProps.get("name")).add(sourceTask);
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
    public void deleteTasksConfig(String worker) {

    }

    @Override
    public void deleteConnectorConfig(String connectorName) {

    }

    @Override
    public Map<String, String> getConnectorConfig(String connectorName) {
        return null;
    }

}
