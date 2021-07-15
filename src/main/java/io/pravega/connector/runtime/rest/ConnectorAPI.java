package io.pravega.connector.runtime.rest;


import io.pravega.connector.runtime.Worker;
import io.pravega.connector.runtime.WorkerState;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.ws.rs.*;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;

import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

@Path("connector")
@Produces(MediaType.APPLICATION_JSON)
@Consumes(MediaType.APPLICATION_JSON)
public class ConnectorAPI {
    private Worker worker;
    private static final Logger log = LoggerFactory.getLogger(ConnectorAPI.class);

    public ConnectorAPI(Worker worker) {
        this.worker = worker;
    }

    @GET
    @Path("{connector}/pause")
    public Response pauseConnector(@PathParam("connector") String connectorName) {
        log.info("pause start");
        worker.setWorkerState(WorkerState.Paused, connectorName);
        return Response.ok().build();
    }

    @GET
    @Path("{connector}/resume")
    public Response resumeConnector(@PathParam("connector") String connectorName) {
        log.info("resume start");
        worker.setWorkerState(WorkerState.Started, connectorName);
        return Response.ok().build();
    }

    @GET
    @Path("{connector}/stop")
    public Response stopConnector(@PathParam("connector") String connectorName) {
        log.info("stop worker");
        worker.setWorkerState(WorkerState.Stopped, connectorName);
        worker.deleteTasksConfig(connectorName);
        worker.deleteConnectorConfig(connectorName);
//        worker.shutdownScheduledService();
        return Response.ok().build();
    }

    @GET
    @Path("{connector}/restart")
    public Response restartConnector(@PathParam("connector") String connectorName) {
        log.info("restart worker not complete");
        ExecutorService threadPool = Executors.newCachedThreadPool();
        Map<String, String> connectorProps = worker.getConnectorConfig(connectorName);
        threadPool.submit(() -> worker.startConnector(connectorProps));
//        worker.startConnector();
        return Response.ok().build();
    }

    @PUT
    @Path("{connector}/config")
//    @Consumes({MediaType.APPLICATION_FORM_URLENCODED})
    public Response updateConfiguration(@PathParam("connector") String connectorName,
                                        Map<String, String> connectorProps) {
        for(String s: connectorProps.keySet()) {
            System.out.println(s);
        }
        return Response.ok().build();

    }



}
