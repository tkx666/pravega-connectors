package io.pravega.connector.runtime.rest;


import io.pravega.connector.runtime.Workers;
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
    private Workers workers;
    private static final Logger log = LoggerFactory.getLogger(ConnectorAPI.class);
    private ExecutorService threadPool = Executors.newCachedThreadPool();


    public ConnectorAPI(Workers workers) {
        this.workers = workers;
    }

    @GET
    @Path("{connector}/pause")
    public Response pauseConnector(@PathParam("connector") String connectorName) {
        log.info("pause start");
        workers.setWorkerState(WorkerState.Paused, connectorName);
        return Response.ok().build();
    }

    @GET
    @Path("{connector}/resume")
    public Response resumeConnector(@PathParam("connector") String connectorName) {
        log.info("resume start");
        workers.setWorkerState(WorkerState.Started, connectorName);
        return Response.ok().build();
    }

    @GET
    @Path("{connector}/stop")
    public Response stopConnector(@PathParam("connector") String connectorName) {
        log.info("stop worker");
        workers.stopConnector(connectorName);
//        worker.shutdownScheduledService();
        return Response.ok().build();
    }

    @GET
    @Path("{connector}/restart")
    public Response restartConnector(@PathParam("connector") String connectorName) {
        log.info("restart worker not complete");
        Map<String, String> connectorProps = workers.getConnectorConfig(connectorName);
        threadPool.submit(() -> workers.startConnector(connectorProps));
//        worker.startConnector();
        return Response.ok().build();
    }

    @PUT
    @Path("{connector}/config")
    public Response updateConfiguration(@PathParam("connector") String connectorName,
                                        Map<String, String> connectorProps) {

        workers.stopConnector(connectorName);
        threadPool.submit(() -> workers.startConnector(connectorProps));

        return Response.ok().build();

    }
}
