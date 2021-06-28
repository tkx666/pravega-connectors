package io.pravega.connecter.runtime.storage;

import io.pravega.connecter.runtime.Task;
import io.pravega.connecter.runtime.WorkerState;

import java.util.Map;

public interface TasksInfoStore {
    Map<String, Task> getTasks (String workerId);
    boolean putTask(String workerId, String taskId, Task task);


}
