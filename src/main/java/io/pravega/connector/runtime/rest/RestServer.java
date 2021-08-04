package io.pravega.connector.runtime.rest;

import com.fasterxml.jackson.jaxrs.json.JacksonJsonProvider;
import io.pravega.connector.runtime.Worker;
import io.pravega.connector.runtime.WorkerConfig;
import org.eclipse.jetty.server.Handler;
import org.eclipse.jetty.server.Server;
import org.eclipse.jetty.server.handler.ContextHandlerCollection;
import org.eclipse.jetty.server.handler.StatisticsHandler;
import org.eclipse.jetty.servlet.ServletContextHandler;
import org.eclipse.jetty.servlet.ServletHolder;
import org.glassfish.jersey.server.ResourceConfig;
import org.glassfish.jersey.servlet.ServletContainer;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

public class RestServer {
    private Server jettyServer;
    private ContextHandlerCollection handlers;
    private Map<String, String> pravegaProps;

    public RestServer(Map<String, String> pravegaProps) {
        this.pravegaProps = pravegaProps;
    }

    public void initializeServer() throws Exception {
        jettyServer = new Server(Integer.parseInt(pravegaProps.get(WorkerConfig.REST_PORT_CONFIG)));
        handlers = new ContextHandlerCollection();
        StatisticsHandler statsHandler = new StatisticsHandler();
        statsHandler.setHandler(handlers);
        jettyServer.setHandler(statsHandler);
        jettyServer.start();


    }

    public void initializeResource(Worker worker) throws Exception {
        ResourceConfig resourceConfig = new ResourceConfig();
        resourceConfig.register(new JacksonJsonProvider());
        resourceConfig.register(new ConnectorAPI(worker));
        ServletContainer servletContainer = new ServletContainer(resourceConfig);
        ServletHolder servletHolder = new ServletHolder(servletContainer);
        List<Handler> contextHandlers = new ArrayList<>();
        ServletContextHandler context = new ServletContextHandler(ServletContextHandler.SESSIONS);
        context.setContextPath("/");
        context.addServlet(servletHolder, "/*");
        contextHandlers.add(context);
        handlers.setHandlers(contextHandlers.toArray(new Handler[0]));
        context.start();

    }

}
