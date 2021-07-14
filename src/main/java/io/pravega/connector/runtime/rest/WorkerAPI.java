package io.pravega.connector.runtime.rest;


import io.pravega.connector.runtime.Worker;
import io.pravega.connector.runtime.WorkerState;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.ws.rs.*;
import javax.ws.rs.core.Response;

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
        log.info("stop worker");
        worker.setWorkerState(WorkerState.Stopped, workerName);
        worker.deleteTasksConfig(workerName);
//        worker.shutdownScheduledService();
        return Response.accepted().build();
    }

    @GET
    @Path("{worker}/restart")
    public Response restartConnector(@PathParam("worker") String workerName) {
        log.info("restart worker");
        worker.execute();
        return Response.accepted().build();
    }



}
