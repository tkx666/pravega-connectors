package io.pravega.connecter.runtime.rest;

import io.pravega.connecter.runtime.Worker;
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

public class RestServer {
    private Server jettyServer;
    private ContextHandlerCollection handlers;

    public void initializeServer() throws Exception {
        jettyServer = new Server(8080);
        handlers = new ContextHandlerCollection();
        StatisticsHandler statsHandler = new StatisticsHandler();
        statsHandler.setHandler(handlers);
        jettyServer.setHandler(statsHandler);
        jettyServer.start();


    }

    public void initializeResource(Worker worker) throws Exception {

//        ServletContextHandler context = new ServletContextHandler(ServletContextHandler.SESSIONS);
//        context.setContextPath("/");
        ResourceConfig resourceConfig = new ResourceConfig();
        resourceConfig.register(new WorkerAPI(worker));
//        Server jettyServer = new Server(8080);
//        jettyServer.setHandler(context);
        ServletContainer servletContainer = new ServletContainer(resourceConfig);
        ServletHolder servletHolder = new ServletHolder(servletContainer);
        List<Handler> contextHandlers = new ArrayList<>();

//
//        ServletHolder jerseyServlet = context.addServlet(
//                org.glassfish.jersey.servlet.ServletContainer.class, "/*");
        ServletContextHandler context = new ServletContextHandler(ServletContextHandler.SESSIONS);
        context.setContextPath("/");
        context.addServlet(servletHolder, "/*");
        contextHandlers.add(context);
        handlers.setHandlers(contextHandlers.toArray(new Handler[0]));
        context.start();



//        jerseyServlet.setInitParameter(
//                "jersey.config.server.provider.classnames",
//                WorkerAPI.class.getCanonicalName());




    }

}
