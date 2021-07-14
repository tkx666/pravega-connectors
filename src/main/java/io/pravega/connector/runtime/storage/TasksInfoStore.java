package io.pravega.connector.runtime.storage;

import io.pravega.connector.runtime.Task;

import java.util.Map;

public interface TasksInfoStore {
    Map<String, Task> getTasks (String workerId);
    boolean putTask(String workerId, String taskId, Task task);


}
