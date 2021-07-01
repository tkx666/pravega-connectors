package io.pravega.connecter.runtime.rest;


import io.pravega.connecter.runtime.Task;
import io.pravega.connecter.runtime.Worker;
import io.pravega.connecter.runtime.WorkerState;
import io.pravega.connecter.runtime.storage.MemoryTasksInfoStore;
import io.pravega.connecter.runtime.storage.TasksInfoStore;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.ws.rs.*;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;
import java.util.Map;

@Path("worker")
public class WorkerAPI {
    private Worker worker;
    private static final Logger log = LoggerFactory.getLogger(WorkerAPI.class);

    public WorkerAPI(Worker worker) {
        this.worker = worker;
    }

    @GET
    @Path("{worker}/pause")
    public Response pauseConnector(@PathParam("worker") String workerName) {
        log.info("pause start");
        worker.setWorkerState(WorkerState.Paused, workerName);
        return Response.accepted().build();
    }

    @GET
    @Path("{worker}/resume")
    public Response resumeConnector(@PathParam("worker") String workerName) {
        log.info("resume start");
        worker.setWorkerState(WorkerState.Started, workerName);
        return Response.accepted().build();
    }

    @GET
    @Path("{worker}/stop")
    public Response stopConnector(@PathParam("worker") String workerName) {
        log.info("stop start");
        worker.setWorkerState(WorkerState.Stopped, workerName);
        return Response.accepted().build();
    }



}
