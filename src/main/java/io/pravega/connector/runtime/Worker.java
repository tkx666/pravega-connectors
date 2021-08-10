package io.pravega.connector.runtime;

import io.pravega.client.admin.ReaderGroupManager;
import io.pravega.client.admin.StreamManager;
import io.pravega.client.stream.*;
import io.pravega.connector.runtime.configs.ConnectorConfig;
import io.pravega.connector.runtime.configs.SinkConfig;
import io.pravega.connector.runtime.configs.SourceConfig;
import io.pravega.connector.runtime.configs.WorkerConfig;
import io.pravega.connector.runtime.sink.SinkTask;
import io.pravega.connector.runtime.source.SourceTask;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.net.URI;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.*;

public class Worker {
    private static final Logger logger = LoggerFactory.getLogger(Worker.class);
    private final ExecutorService executor;
    private final ScheduledExecutorService scheduledExecutorService;
    private WorkerConfig workerConfig;
    private Map<String, List<Task>> tasks;
    private Map<String, Map<String, String>> connectors;

    private volatile ConnectorState state;
    public static String CHECK_POINT_INTERVAL = "30";


    public Worker(WorkerConfig workerConfig) {
        this.workerConfig = workerConfig;
        this.executor = new ThreadPoolExecutor(20, 200, 60L, TimeUnit.SECONDS, new LinkedBlockingDeque<Runnable>());
        this.state = state;
        this.tasks = new HashMap<>();
        this.scheduledExecutorService = Executors.newScheduledThreadPool(10);
        this.connectors = new HashMap<>();

    }

    /**
     *
     * @param connectorProps
     */

    public void startConnector(Map<String, String> connectorProps) {
        try {
//            WorkerConfig.config.validate(pravegaProps);
            startTasks(connectorProps);
            if (connectorProps.get(ConnectorConfig.TYPE_CONFIG).equals("sink")) {
                startCheckPoint(connectorProps);
            }
        } catch (Exception e) {
            logger.error("start connector error", e);
        }

    }

    public void startTasks(Map<String, String> connectorProps) {
        try {
            if (connectorProps.get(ConnectorConfig.TYPE_CONFIG).equals("source")) {
//                SourceBasicConfig.basicConfig.validate(connectorProps);
                SourceConfig sourceConfig = new SourceConfig(connectorProps);
                int threadNum = sourceConfig.getInt(SourceConfig.TASKS_NUM_CONFIG);
                createScopeAndStream();
                for (int i = 0; i < threadNum; i++) {
                    int id = i;
                    SourceTask sourceTask = new SourceTask(sourceConfig, workerConfig, ConnectorState.Started, id);
                    sourceTask.initialize();
                    tasks.putIfAbsent(sourceConfig.getString(SourceConfig.NAME_CONFIG), new ArrayList<>());
                    tasks.get(sourceConfig.getString(SourceConfig.NAME_CONFIG)).add(sourceTask);
                    executor.submit(sourceTask);
                }
            } else if (connectorProps.get(ConnectorConfig.TYPE_CONFIG).equals("sink")) {
//                SinkBasicConfig.basicConfig.validate(connectorProps);
                SinkConfig sinkConfig = new SinkConfig(connectorProps);
                int threadNum = sinkConfig.getInt(SinkConfig.TASKS_NUM_CONFIG);
                String scope = workerConfig.getString(WorkerConfig.SCOPE_CONFIG);
                URI controllerURI = URI.create(workerConfig.getString(WorkerConfig.URI_CONFIG));
                String readerGroup = workerConfig.getString(WorkerConfig.READER_GROUP_CONFIG);
                createScopeAndStream();
                createReaderGroup();
                if (hasCheckPoint(connectorProps)) {
                    FileChannel fileChannel = new FileInputStream(sinkConfig.getString(SinkConfig.CHECKPOINT_PERSIST_PATH_CONFIG)).getChannel();
                    ByteBuffer buffer = ByteBuffer.allocate(1024);
                    fileChannel.read(buffer);
                    buffer.flip();
                    Checkpoint checkpoint = Checkpoint.fromBytes(buffer);
                    logger.info("check point recover: " + checkpoint.getName());
                    ReaderGroupManager readerGroupManager = ReaderGroupManager.withScope(scope, controllerURI);
                    ReaderGroup existingGroup = readerGroupManager.getReaderGroup(readerGroup);
                    existingGroup.resetReaderGroup(ReaderGroupConfig
                            .builder()
                            .startFromCheckpoint(checkpoint)
                            .build());
                    existingGroup.close();
                    readerGroupManager.close();
                }
                for (int i = 0; i < threadNum; i++) {
                    int id = i;
                    SinkTask sinkTask = new SinkTask(sinkConfig, workerConfig, ConnectorState.Started, id);
                    sinkTask.initialize();
                    tasks.putIfAbsent(sinkConfig.getString(SinkConfig.NAME_CONFIG), new ArrayList<>());
                    tasks.get(sinkConfig.getString(SinkConfig.NAME_CONFIG)).add(sinkTask);
                    executor.submit(sinkTask);
                }
            }
            connectors.put(connectorProps.get(ConnectorConfig.NAME_CONFIG), connectorProps);
        } catch (Exception e) {
            logger.error("start task error", e);
        }

    }

    private void createScopeAndStream() {
        String scope = workerConfig.getString(WorkerConfig.SCOPE_CONFIG);
        String streamName = workerConfig.getString(WorkerConfig.STREAM_NAME_CONFIG);
        URI controllerURI = URI.create(workerConfig.getString(WorkerConfig.URI_CONFIG));
        StreamManager streamManager = StreamManager.create(controllerURI);
        streamManager.createScope(scope);
        StreamConfiguration streamConfig = StreamConfiguration.builder()
                .scalingPolicy(ScalingPolicy.fixed(workerConfig.getInt(WorkerConfig.SEGMENTS_NUM_CONFIG)))
                .build();
        streamManager.createStream(scope, streamName, streamConfig);
        streamManager.close();
    }

    private void createReaderGroup() {
        String scope = workerConfig.getString(WorkerConfig.SCOPE_CONFIG);
        String streamName = workerConfig.getString(WorkerConfig.STREAM_NAME_CONFIG);
        URI controllerURI = URI.create(workerConfig.getString(WorkerConfig.URI_CONFIG));
        String readerGroup = workerConfig.getString(WorkerConfig.READER_GROUP_CONFIG);
        final ReaderGroupConfig readerGroupConfig = ReaderGroupConfig.builder()
                .stream(Stream.of(scope, streamName))
                .build();
        try (ReaderGroupManager readerGroupManager = ReaderGroupManager.withScope(scope, controllerURI)) {
            readerGroupManager.createReaderGroup(readerGroup, readerGroupConfig);
        }
    }

    private boolean hasCheckPoint(Map<String, String> connectorProps) {
        File file = new File(connectorProps.get(SinkConfig.CHECKPOINT_PERSIST_PATH_CONFIG));
        String enable = connectorProps.get(SinkConfig.CHECKPOINT_ENABLE_CONFIG);
        return enable.equals("true") && file.exists();
    }


    public void startCheckPoint(Map<String, String> sinkProps) {
        if (!sinkProps.containsKey(SinkConfig.CHECKPOINT_ENABLE_CONFIG) || !sinkProps.get(SinkConfig.CHECKPOINT_ENABLE_CONFIG).equals("true"))
            return;
        ReaderGroupManager readerGroupManager = ReaderGroupManager.withScope(workerConfig.getString(WorkerConfig.SCOPE_CONFIG), URI.create(workerConfig.getString(WorkerConfig.URI_CONFIG)));
        ReaderGroup group = readerGroupManager.getReaderGroup(workerConfig.getString(WorkerConfig.READER_GROUP_CONFIG));
        ScheduledFuture<?> future = scheduledExecutorService.scheduleAtFixedRate(new Runnable() {
            @Override
            public void run() {
                logger.info("reader group {}", group.getOnlineReaders());
                CompletableFuture<Checkpoint> checkpointResult =
                        group.initiateCheckpoint(sinkProps.get(SinkConfig.CHECKPOINT_NAME_CONFIG), Executors.newScheduledThreadPool(5));
                try {
                    Checkpoint checkpoint = checkpointResult.get(10, TimeUnit.SECONDS);
                    logger.info("check point start {}", checkpoint);
                    ByteBuffer serializedCheckPoint = checkpoint.toBytes();
                    FileChannel fileChannel = new FileOutputStream(sinkProps.get(SinkConfig.CHECKPOINT_PERSIST_PATH_CONFIG)).getChannel();

                    fileChannel.write(serializedCheckPoint);
                    fileChannel.close();
                } catch (Exception e) {
                    e.printStackTrace();
                } finally {
                    group.close();
                    readerGroupManager.close();
                }

            }
        }, 0, Long.parseLong(CHECK_POINT_INTERVAL), TimeUnit.SECONDS);

    }

    public void setConnectorState(ConnectorState connectorState, String workerName) {
        this.state = connectorState;
        List<Task> tasksList = tasks.get(workerName);
        if (tasksList == null) return;
        for (Task task : tasksList) {
            task.setState(connectorState);
        }
    }

    public void deleteTasksConfig(String connectorName) {
        tasks.remove(connectorName);
    }

    void deleteConnectorConfig(String connectorName) {
        connectors.remove(connectorName);
    }

    public void stopConnector(String connectorName) {
        setConnectorState(ConnectorState.Stopped, connectorName);
        deleteTasksConfig(connectorName);
        deleteConnectorConfig(connectorName);
    }

    public Map<String, String> getConnectorConfig(String connectorName) {
        return connectors.get(connectorName);
    }

    public void addShutDownHook() {
        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            logger.info("shutdown connector");
            for (String connector : connectors.keySet()) {
                stopConnector(connector);
            }
        }));
    }
}
