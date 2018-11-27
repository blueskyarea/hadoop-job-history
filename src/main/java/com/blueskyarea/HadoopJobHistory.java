package com.blueskyarea;

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.io.PrintWriter;

import javax.servlet.ServletException;
import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import org.apache.log4j.Logger;
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
import com.blueskyarea.generator.JobHistoryReader;

public class HadoopJobHistory {
	private static final Logger LOG = Logger.getLogger("HadoopJobHistory");
	private static HadoopJobHistoryConfig config = HadoopJobHistoryConfig
			.getInstance();
	private static JobHistoryGenerator realtimeGenerator = new JobHistoryGenerator(
			config);
	private static JobHistoryReader historyReader = new JobHistoryReader();
	public static String thisJarDirPath = HadoopJobHistory.getJarPath();
	public static String historyFilePath = thisJarDirPath + "/../history.txt";

	public static void main(String[] args) throws InterruptedException {		
		// create servlet
		ServletContextHandler servletHandler = new ServletContextHandler(
				ServletContextHandler.SESSIONS);
		servletHandler.addServlet(new ServletHolder(new HistoryServlet()),
				"/api");

		// specify static contents
		final ResourceHandler resourceHandler = new ResourceHandler();
		resourceHandler.setResourceBase(thisJarDirPath + "/view");
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
		final HttpConnectionFactory httpConnFactory = new HttpConnectionFactory(
				httpConfig);
		final ServerConnector httpConnector = new ServerConnector(jettyServer,
				httpConnFactory);
		httpConnector.setPort(config.getAppPort());
		jettyServer.setConnectors(new Connector[] { httpConnector });

		// start server
		try {
			jettyServer.start();

			// start thread for history
			Runnable r = () -> {
				while (true) {
					try {
						String result = realtimeGenerator.startToGetHistory();
						File file = new File(historyFilePath);
						FileWriter filewriter = new FileWriter(file);
						filewriter.write(result);
						filewriter.close();
						Thread.sleep(60000L);
					} catch (Exception e) {
						// nothing to do
					}
				}
			};

			Thread thread = new Thread(r);
			thread.start();
			jettyServer.join();
			thread.join();
		} catch (Exception e) {
			throw new RuntimeException(e);
		}

	}
	
	public static String getJarPath() {
        final String classFileName = "/" + HadoopJobHistory.class.getName().replaceAll("\\.", "/") + ".class";
        final String classFilePath = HadoopJobHistory.class.getResource(classFileName).getPath();
        final File jarFilePath = new File(classFilePath.replaceFirst("!/.*$", ""));
        return jarFilePath.getParent().replaceFirst("file:", "");
	}

	public static class HistoryServlet extends HttpServlet {
		private static final long serialVersionUID = 7780393067807928828L;

		@Override
		protected void doGet(HttpServletRequest req, HttpServletResponse resp)
				throws ServletException, IOException {
			LOG.info("requestUrl: " + req.getRequestURI());
			final String dn = req.getParameter("dn");
			LOG.info("dn: " + dn);
			resp.addHeader("Access-Control-Allow-Origin", "*");
			resp.addHeader("Access-Control-Allow-Headers", "Content-Type");
			resp.setContentType("application/json; charset=UTF-8");
			final PrintWriter out = resp.getWriter();
			switch (dn) {
			case "real":
				out.println(realtimeGenerator.startToGetList());
				out.close();
				break;
			case "hist":
				out.println(historyReader.readLatestHistoryAsJson());
				out.close();
				break;
			default:
				LOG.info("unexpected parameter dn: " + dn);
			}
		}
	}
}
