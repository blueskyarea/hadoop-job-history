package com.blueskyarea;

import java.io.File;
import java.io.IOException;
import java.io.PrintWriter;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

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
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.blueskyarea.config.HadoopResultSaverConfig;
import com.blueskyarea.exception.HadoopJobHistorySaverRuntimeException;
import com.blueskyarea.generator.JobHistoryReader;
import com.blueskyarea.thread.JobHistoryThread;

public class HadoopJobHistorySaver {
	private static final Logger LOG = LoggerFactory
			.getLogger("HadoopJobHistorySaver");
	private static HadoopResultSaverConfig config = HadoopResultSaverConfig
			.getInstance();
	private static String thisJarDirPath = HadoopJobHistorySaver.getJarPath();
	private static JobHistoryReader historyReader = new JobHistoryReader(config);

	public static void main(String[] args) throws InterruptedException {
		LOG.info("HadoopJobHistorySaver started as independent application.");
		new HadoopJobHistorySaver().start();
	}

	/**
	 * This is public for using as library also.
	 */
	public void start() {
		final Server jettyServer = new Server();
		final HandlerList handlerList = createHandlerList();
		final HttpConnectionFactory httpConnFactory = createHttpConnectionFactory();
		final ServerConnector httpConnector = new ServerConnector(jettyServer,
				httpConnFactory);
		httpConnector.setPort(config.getAppPort());
		jettyServer.setHandler(handlerList);
		jettyServer.setConnectors(new Connector[] { httpConnector });

		try {
			// start ExecutorService for getting history
			ExecutorService s = Executors.newSingleThreadExecutor();
			s.submit(new JobHistoryThread(config));

			// start jetty server for web application
			jettyServer.start();
			jettyServer.join();
		} catch (Exception e) {
			throw new HadoopJobHistorySaverRuntimeException(e.getMessage());
		}
	}

	protected HandlerList createHandlerList() {
		// ServletContextHandler for servlet
		final ServletContextHandler servletHandler = new ServletContextHandler(
				ServletContextHandler.SESSIONS);
		servletHandler.addServlet(new ServletHolder(new HistoryServlet()),
				"/api");

		// ResourceHandler for specify static contents
		final ResourceHandler resourceHandler = new ResourceHandler();
		LOG.info("thisJarDirPath : " + thisJarDirPath);
		resourceHandler.setResourceBase(thisJarDirPath + "/view");
		resourceHandler.setDirectoriesListed(false);
		resourceHandler.setWelcomeFiles(new String[] { "hist.html" });
		resourceHandler.setCacheControl("no-store,no-cache,must-revalidate");

		// Set HandlerList for jetty server
		final HandlerList handlerList = new HandlerList();
		handlerList.addHandler(resourceHandler);
		handlerList.addHandler(servletHandler);

		return handlerList;
	}

	protected HttpConnectionFactory createHttpConnectionFactory() {
		// HttpConfiguration for HttpConnectionFactory
		final HttpConfiguration httpConfig = new HttpConfiguration();
		// hide version info
		httpConfig.setSendServerVersion(false);

		// HttpConnectionFactory for ServerConnector
		return new HttpConnectionFactory(httpConfig);
	}

	protected static String getJarPath() {
		final String classFileName = "/"
				+ HadoopJobHistorySaver.class.getName().replaceAll("\\.", "/")
				+ ".class";
		final String classFilePath = HadoopJobHistorySaver.class.getResource(
				classFileName).getPath();
		final File jarFilePath = new File(classFilePath.replaceFirst("!/.*$",
				""));
		return jarFilePath.getParent().replaceFirst("file:", "");
	}

	public static class HistoryServlet extends HttpServlet {
		private static final long serialVersionUID = 7780393067807928828L;

		@Override
		protected void doGet(HttpServletRequest req, HttpServletResponse resp)
				throws ServletException, IOException {
			LOG.info("Requested URL: " + req.getRequestURI());
			final String dn = req.getParameter("dn");
			final String ap = req.getParameter("ap");
			final String fromDateTime = 
					req.getParameter("fromd").replace("-", "/")
					+ " " + req.getParameter("fromt");
			final String toDateTime = 
					req.getParameter("tod").replace("-", "/") 
					+ " " + req.getParameter("tot");

			LOG.info("dn: " + dn);
			LOG.info("ap: " + ap);
			LOG.info("fromDateTime: " + fromDateTime);
			LOG.info("toDateTime: " + toDateTime);
			resp.addHeader("Access-Control-Allow-Origin", "*");
			resp.addHeader("Access-Control-Allow-Headers", "Content-Type");
			resp.setContentType("application/json; charset=UTF-8");
			final PrintWriter out = resp.getWriter();
			switch (dn) {
			case "hist":
				out.println(historyReader.readLatestHistoryAsJson(ap,
						fromDateTime, toDateTime));
				out.close();
				break;
			default:
				LOG.info("unexpected parameter dn: " + dn);
			}
		}
	}
}
