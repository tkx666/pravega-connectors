package io.pravega.connector.runtime;

import io.pravega.client.admin.ReaderGroupManager;
import io.pravega.client.admin.StreamManager;
import io.pravega.client.stream.*;
import io.pravega.connector.runtime.sink.Sink;
import io.pravega.connector.runtime.sink.SinkTask;
import io.pravega.connector.runtime.source.Source;
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
    private Map<String, String> pravegaProps;
    private Map<String, List<Task>> tasks;
    private Map<String, Map<String, String>> connectors;

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
    public static String TYPE_CONFIG = "type";
    public static String SINK_CLASS_CONFIG = "class";
    public static String SINK_NAME_CONFIG = "name";
    public static String CHECK_POINT_INTERVAL = "30";
    public static String READER_GROUP_NAME_CONFIG = "readerGroup";
    public static String CHECK_POINT_NAME = "checkpoint.name";
    public static String CHECK_POINT_PATH_CONFIG = "checkpoint.persist.path";


    public Worker(Map<String, String> pravegaProps) {
        this.pravegaProps = pravegaProps;
        this.executor = new ThreadPoolExecutor(20, 200, 60L, TimeUnit.SECONDS, new LinkedBlockingDeque<Runnable>());
        this.workerState = workerState;
        this.tasks = new HashMap<>();
        this.scheduledExecutorService = Executors.newScheduledThreadPool(10);
        this.connectors = new HashMap<>();

    }

    public void startConnector(Map<String, String> connectorProps) {
        startTasks(connectorProps);
        if (connectorProps.get(TYPE_CONFIG).equals("sink")) startCheckPoint(connectorProps);
    }

    public void startTasks(Map<String, String> connectorProps) {
        try {
            connectors.put(connectorProps.get(CONNECT_NAME_CONFIG), connectorProps);
            if (connectorProps.get(TYPE_CONFIG).equals("source")) {
                Class<?> sourceClass = null;
                int threadNum = Integer.parseInt(connectorProps.get(TASK_NUM_CONFIG));
                List<Source> sourceGroup = new ArrayList<>();
                createScopeAndStream();

                for (int i = 0; i < threadNum; i++) {
                    sourceClass = Class.forName(connectorProps.get(SOURCE_CLASS_CONFIG));
                    Source source = (Source) sourceClass.newInstance();
                    source.open(connectorProps, pravegaProps);
                    sourceGroup.add(source);
                }
                for (int i = 0; i < threadNum; i++) {
                    Writer writer = null;
                    if (connectorProps.containsKey(TRANSACTION_ENABLE_CONFIG) && connectorProps.get(TRANSACTION_ENABLE_CONFIG).equals("true")) {
                        writer = new PravegaTransactionalWriter(pravegaProps);
                    } else
                        writer = new PravegaWriter(pravegaProps);
                    writer.initialize();
                    SourceTask sourceTask = new SourceTask(writer, sourceGroup.get(i), pravegaProps, WorkerState.Started);
                    tasks.putIfAbsent(connectorProps.get(CONNECT_NAME_CONFIG), new ArrayList<>());
                    tasks.get(connectorProps.get(CONNECT_NAME_CONFIG)).add(sourceTask);
                    executor.submit(sourceTask);
                }
            } else if (connectorProps.get(TYPE_CONFIG).equals("sink")) {
                Class<?> sinkClass = null;
                int threadNum = Integer.parseInt(connectorProps.get(TASK_NUM_CONFIG));
                sinkClass = Class.forName(connectorProps.get(SINK_CLASS_CONFIG));
                String scope = pravegaProps.get(SCOPE_CONFIG);
                URI controllerURI = URI.create(pravegaProps.get(URI_CONFIG));
                String readerGroup = pravegaProps.get(READER_GROUP_NAME_CONFIG);
                createScopeAndStream();
                createReaderGroup();
                if (hasCheckPoint(connectorProps)) {
                    FileChannel fileChannel = new FileInputStream(connectorProps.get(CHECK_POINT_PATH_CONFIG)).getChannel();
                    ByteBuffer buffer = ByteBuffer.allocate(1024);
                    fileChannel.read(buffer);
                    buffer.flip();
                    Checkpoint checkpoint = Checkpoint.fromBytes(buffer);
                    System.out.println("check point recover: " + checkpoint.getName());
                    ReaderGroupManager readerGroupManager = ReaderGroupManager.withScope(scope, controllerURI);
                    ReaderGroup existingGroup = readerGroupManager.getReaderGroup(readerGroup);
                    existingGroup.resetReaderGroup(ReaderGroupConfig
                            .builder()
                            .startFromCheckpoint(checkpoint)
                            .build());
                }
                List<PravegaReader> readerGroupList = new ArrayList<>();
                for (int i = 0; i < threadNum; i++) {
                    PravegaReader reader = new PravegaReader(pravegaProps, connectorProps.get(SINK_NAME_CONFIG) + i);
                    reader.initialize(pravegaProps);
                    readerGroupList.add(reader);
                }
                for (int i = 0; i < threadNum; i++) {
                    Sink sink = (Sink) sinkClass.newInstance();
                    sink.open(connectorProps, pravegaProps);
                    SinkTask sinkTask = new SinkTask(readerGroupList.get(i), sink, pravegaProps, WorkerState.Started);
                    tasks.putIfAbsent(connectorProps.get("name"), new ArrayList<>());
                    tasks.get(connectorProps.get("name")).add(sinkTask);
                    executor.submit(sinkTask);
                }
            }
        } catch (Exception e) {
            e.printStackTrace();
        }

    }

    private void createScopeAndStream() {
        String scope = pravegaProps.get(SCOPE_CONFIG);
        String streamName = pravegaProps.get(STREAM_NAME_CONFIG);
        URI controllerURI = URI.create(pravegaProps.get(URI_CONFIG));
        StreamManager streamManager = StreamManager.create(controllerURI);
        streamManager.createScope(scope);
        StreamConfiguration streamConfig = StreamConfiguration.builder()
                .scalingPolicy(ScalingPolicy.fixed(Integer.parseInt(pravegaProps.get(SEGMENTS_NUM_CONFIG))))
                .build();
        streamManager.createStream(scope, streamName, streamConfig);
        streamManager.close();
    }

    private void createReaderGroup() {
        String scope = pravegaProps.get(SCOPE_CONFIG);
        String streamName = pravegaProps.get(STREAM_NAME_CONFIG);
        URI controllerURI = URI.create(pravegaProps.get(URI_CONFIG));
        String readerGroup = pravegaProps.get(READER_GROUP_NAME_CONFIG);
        final ReaderGroupConfig readerGroupConfig = ReaderGroupConfig.builder()
                .stream(Stream.of(scope, streamName))
                .build();
        try (ReaderGroupManager readerGroupManager = ReaderGroupManager.withScope(scope, controllerURI)) {
            readerGroupManager.createReaderGroup(readerGroup, readerGroupConfig);
        }
    }

    private boolean hasCheckPoint(Map<String, String> connectorProps) {
        File file = new File(connectorProps.get(CHECK_POINT_PATH_CONFIG));
        return file.exists();
    }


    public void startCheckPoint(Map<String, String> sinkProps) {
        ReaderGroupManager readerGroupManager = ReaderGroupManager.withScope(pravegaProps.get(SCOPE_CONFIG), URI.create(pravegaProps.get(URI_CONFIG)));
        ReaderGroup group = readerGroupManager.getReaderGroup(pravegaProps.get(READER_GROUP_NAME_CONFIG));
        ScheduledFuture<?> future = scheduledExecutorService.scheduleAtFixedRate(new Runnable() {
            @Override
            public void run() {
                logger.info("reader group {}", group.getOnlineReaders());
                CompletableFuture<Checkpoint> checkpointResult =
                        group.initiateCheckpoint(sinkProps.get(CHECK_POINT_NAME), Executors.newScheduledThreadPool(5));
                try {
                    Checkpoint checkpoint = checkpointResult.get(10, TimeUnit.SECONDS);
                    logger.info("check point start {}", checkpoint);
                    ByteBuffer serializedCheckPoint = checkpoint.toBytes();
                    FileChannel fileChannel = new FileOutputStream(sinkProps.get(CHECK_POINT_PATH_CONFIG)).getChannel();

                    fileChannel.write(serializedCheckPoint);
                    fileChannel.close();
                } catch (Exception e) {
                    e.printStackTrace();
                }

            }
        }, 0, Long.parseLong(CHECK_POINT_INTERVAL), TimeUnit.SECONDS);
    }

    public void setWorkerState(WorkerState workerState, String workerName) {
        this.workerState = workerState;
        List<Task> tasksList = tasks.get(workerName);
        if (tasksList == null) return;
        for (Task task : tasksList) {
            task.setState(workerState);
        }
    }

    public void deleteTasksConfig(String connectorName) {
        tasks.remove(connectorName);
    }

    void deleteConnectorConfig(String connectorName) {
        connectors.remove(connectorName);
    }

    public void stopConnector(String connectorName) {
        setWorkerState(WorkerState.Stopped, connectorName);
        deleteTasksConfig(connectorName);
        deleteConnectorConfig(connectorName);
    }

    public Map<String, String> getConnectorConfig(String connectorName) {
        return connectors.get(connectorName);
    }
}
