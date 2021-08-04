package io.pravega.connector.runtime;

import java.util.Map;

public class SinkBasicConfig extends AbstractConfig{
    public static String TYPE_CONFIG = "type";
    public static String TASKS_NUM_CONFIG = "tasks.max";
    public static String NAME_CONFIG = "name";
    public static String CLASS_CONFIG = "class";
    public static String CHECKPOINT_PERSIST_PATH_CONFIG = "checkpoint.persist.path";
    public static String CHECKPOINT_NAME_CONFIG = "checkpoint.name";
    public static String CHECKPOINT_ENABLE_CONFIG = "checkpoint.enable";

    static Config.Validator validator = new Config.NonEmptyStringValidator();

    public static final Config basicConfig = new Config()
            .add(TYPE_CONFIG, Config.Type.STRING, "sink", validator)
            .add(TASKS_NUM_CONFIG, Config.Type.STRING, "1", validator)
            .add(NAME_CONFIG, Config.Type.STRING, null, null)
            .add(CLASS_CONFIG, Config.Type.STRING, null, null)
            .add(CHECKPOINT_PERSIST_PATH_CONFIG, Config.Type.STRING, null, null)
            .add(CHECKPOINT_NAME_CONFIG, Config.Type.STRING, null, null)
            .add(CHECKPOINT_ENABLE_CONFIG, Config.Type.STRING, "true", validator);
    public SinkBasicConfig(Map<String, String> props) {
        super(basicConfig,  props);
    }

}
