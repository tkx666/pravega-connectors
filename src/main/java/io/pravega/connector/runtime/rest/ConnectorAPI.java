package io.pravega.connector.runtime.rest;


import io.pravega.connector.runtime.Worker;
import io.pravega.connector.runtime.ConnectorState;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.ws.rs.*;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;

import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

@Path("connectors")
@Produces(MediaType.APPLICATION_JSON)
@Consumes(MediaType.APPLICATION_JSON)
public class ConnectorAPI {
    private Worker worker;
    private static final Logger log = LoggerFactory.getLogger(ConnectorAPI.class);
    private ExecutorService threadPool = Executors.newCachedThreadPool();


    public ConnectorAPI(Worker worker) {
        this.worker = worker;
    }

    /**
     * Get the configuration of the specific connector
     *
     * @param connectorName
     * @return
     */
    @GET
    @Path("{connector}/config")
    public Response getConnectorConfig(@PathParam("connector") String connectorName) {
        Map<String, String> res = worker.getConnectorConfig(connectorName);
        return Response.ok(res).build();
    }

    /**
     * set the state of connector to pause
     *
     * @param connectorName
     * @return
     */
    @PUT
    @Path("{connector}/pause")
    public Response pauseConnector(@PathParam("connector") String connectorName) {
        log.info("pause start");
        worker.setConnectorState(ConnectorState.Paused, connectorName);
        return Response.ok().build();
    }

    /**
     * set the state of connector to resume
     *
     * @param connectorName
     * @return
     */
    @PUT
    @Path("{connector}/resume")
    public Response resumeConnector(@PathParam("connector") String connectorName) {
        log.info("resume start");
        worker.setConnectorState(ConnectorState.Started, connectorName);
        return Response.ok().build();
    }

    /**
     * set the state of connector to stopped
     *
     * @param connectorName
     * @return
     */
    @PUT
    @Path("{connector}/stop")
    public Response stopConnector(@PathParam("connector") String connectorName) {
        log.info("stop worker");
        worker.stopConnector(connectorName);
        return Response.ok().build();
    }

    /**
     * delete the connector
     *
     * @param connectorName
     * @return
     */
    @DELETE
    @Path("{connector}")
    public Response deleteConnector(@PathParam("connector") String connectorName) {
        log.info("stop worker");
        worker.stopConnector(connectorName);
        return Response.ok().build();
    }

    /**
     * restart the connector
     *
     * @param connectorName
     * @return
     */
    @POST
    @Path("{connector}/restart")
    public Response restartConnector(@PathParam("connector") String connectorName) {
        Map<String, String> connectorProps = worker.getConnectorConfig(connectorName);
        threadPool.submit(() -> worker.startConnector(connectorProps));
        return Response.ok().build();
    }

    /**
     * update the config of the connector
     *
     * @param connectorName
     * @param connectorProps the new configuration of connector
     * @return
     */
    @PUT
    @Path("{connector}/config")
    public Response updateConfiguration(@PathParam("connector") String connectorName,
                                        Map<String, String> connectorProps) {

        worker.stopConnector(connectorName);
        threadPool.submit(() -> worker.startConnector(connectorProps));

        return Response.ok().build();

    }
}
