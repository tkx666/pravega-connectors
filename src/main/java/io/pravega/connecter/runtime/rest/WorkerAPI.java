package io.pravega.connecter.runtime.rest;


import io.pravega.connecter.runtime.Task;
import io.pravega.connecter.runtime.storage.MemoryTasksInfoStore;
import io.pravega.connecter.runtime.storage.TasksInfoStore;

import javax.ws.rs.*;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;
import java.util.Map;

@Path("worker")
public class WorkerAPI {

    @GET
    @Path("{worker}/pause")
    public Response pauseConnector(@PathParam("worker") String worker) {
        System.out.println("pause start");
        TasksInfoStore tasksInfoStore = new MemoryTasksInfoStore();
        Map<String, Task> tasks = tasksInfoStore.getTasks(worker);
        System.out.println(tasks.get("1"));

        return Response.accepted().build();
    }



}
