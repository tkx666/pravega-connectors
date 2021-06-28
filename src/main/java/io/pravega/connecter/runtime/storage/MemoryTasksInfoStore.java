package io.pravega.connecter.runtime.storage;

import io.pravega.connecter.runtime.Task;
import io.pravega.connecter.runtime.WorkerState;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class MemoryTasksInfoStore implements TasksInfoStore {
    static Map<String, Map<String, Task>> tasksMap = new HashMap<>();
    public MemoryTasksInfoStore() {
    }
    @Override
    public Map<String, Task> getTasks(String workerId) {
        if(tasksMap.containsKey(workerId)) {
            return tasksMap.get(workerId);
        }
        else {
            return null;
        }
    }

    @Override
    public boolean putTask(String workerId, String taskId, Task task) {
        if(!tasksMap.containsKey(workerId)) {
            tasksMap.put(workerId, new HashMap<>());
        }
        Map<String, Task> tasksStatus = tasksMap.get(workerId);
        tasksStatus.put(taskId, task);
        return true;

    }

}
