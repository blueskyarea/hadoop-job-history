package com.blueskyarea;

import java.io.IOException;
import java.io.PrintWriter;

import javax.servlet.ServletException;
import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import org.eclipse.jetty.server.Connector;
import org.eclipse.jetty.server.HttpConfiguration;
import org.eclipse.jetty.server.HttpConnectionFactory;
import org.eclipse.jetty.server.Server;
import org.eclipse.jetty.server.ServerConnector;
import org.eclipse.jetty.server.handler.HandlerList;
import org.eclipse.jetty.server.handler.ResourceHandler;
import org.eclipse.jetty.servlet.ServletContextHandler;
import org.eclipse.jetty.servlet.ServletHolder;

import com.blueskyarea.config.HadoopJobHistoryConfig;
import com.blueskyarea.generator.JobHistoryGenerator;

public class HadoopJobHistory {
	private static HadoopJobHistoryConfig config = HadoopJobHistoryConfig.getInstance();
	private static JobHistoryGenerator realtimeGenerator = new JobHistoryGenerator(config);
	
    public static void main( String[] args ){
    	// create servlet
    	ServletContextHandler servletHandler = new ServletContextHandler(ServletContextHandler.SESSIONS);
    	servletHandler.addServlet(new ServletHolder(new HistoryServlet()), "/api");
    	
    	// specify static contents
    	final ResourceHandler resourceHandler = new ResourceHandler();
        resourceHandler.setResourceBase(System.getProperty("user.dir") + "/view");
        resourceHandler.setDirectoriesListed(false);
        resourceHandler.setWelcomeFiles(new String[] { "index.html" });
        resourceHandler.setCacheControl("no-store,no-cache,must-revalidate");
        
        HandlerList handlerList = new HandlerList();
        handlerList.addHandler(resourceHandler);
        handlerList.addHandler(servletHandler);
        
        final Server jettyServer = new Server();
        jettyServer.setHandler(handlerList);
        
        // hide version info
        final HttpConfiguration httpConfig = new HttpConfiguration();
        httpConfig.setSendServerVersion(false);
        final HttpConnectionFactory httpConnFactory = new HttpConnectionFactory(httpConfig);
        final ServerConnector httpConnector = new ServerConnector(jettyServer, httpConnFactory);
        httpConnector.setPort(config.getAppPort());
        jettyServer.setConnectors(new Connector[] { httpConnector });
        
        // start server
        try {
            jettyServer.start();
            jettyServer.join();
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }
    
    public static class HistoryServlet extends HttpServlet {
		private static final long serialVersionUID = 7780393067807928828L;
    	
		@Override
        protected void doGet(HttpServletRequest req, HttpServletResponse resp) throws ServletException, IOException {
			final String paramMessage = req.getParameter("message");
			resp.addHeader("Access-Control-Allow-Origin", "*");
            resp.addHeader("Access-Control-Allow-Headers", "Content-Type");
            resp.setContentType("application/json; charset=UTF-8");
            final PrintWriter out = resp.getWriter();
            out.println(realtimeGenerator.startToGetList());
            out.close();
		}
    }
}
