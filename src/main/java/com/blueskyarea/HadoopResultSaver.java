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
import com.blueskyarea.config.HadoopResultSaverConfig2;
import com.blueskyarea.generator.JobHistoryGenerator;
import com.blueskyarea.generator.JobHistoryReader;
import com.blueskyarea.thread.JobHistoryThread;

public class HadoopResultSaver {
	private static final Logger LOG = LoggerFactory.getLogger("HadoopResultSaver");
	//private static HadoopResultSaverConfig config = HadoopResultSaverConfig
	//		.getInstance();
	private static HadoopResultSaverConfig2 config2 = HadoopResultSaverConfig2.getInstance();
	private static JobHistoryReader historyReader = new JobHistoryReader();
	public static JobHistoryGenerator realtimeGenerator = new JobHistoryGenerator(config2);
	public static String thisJarDirPath = HadoopResultSaver.getJarPath();
	public static String historyFilePath = config2.getHistoryFilePath();

	public static void main(String[] args) throws InterruptedException {
		//new HadoopResultSaverConfig2();
		
		// create servlet
		ServletContextHandler servletHandler = new ServletContextHandler(
				ServletContextHandler.SESSIONS);
		servletHandler.addServlet(new ServletHolder(new HistoryServlet()),
				"/api");

		// specify static contents
		final ResourceHandler resourceHandler = new ResourceHandler();
		LOG.info("thisJarDirPath : " + thisJarDirPath);
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
		//httpConnector.setPort(config.getAppPort());
		httpConnector.setPort(config2.getAppPort());
		jettyServer.setConnectors(new Connector[] { httpConnector });

		// start server
		try {
			//jettyServer.start();
			//(new Thread(new JobHistoryThread(10))).start();
	        ExecutorService s = Executors.newSingleThreadExecutor();
	        s.submit(new JobHistoryThread(config2));
			
			// start thread for getting history
			/*Runnable r = () -> {
				//while (true) {
					try {
						String result = realtimeGenerator.startToGetHistory();
						File file = new File(historyFilePath);
						FileWriter filewriter = new FileWriter(file);
						filewriter.write(result);
						filewriter.close();
						Thread.sleep(10000L);
					} catch (Exception e) {
						// nothing to do
					}
				//}
			};*/

			//Thread thread = new Thread(r);
			//thread.start();
			//jettyServer.join();
			//thread.join();
	        jettyServer.start();
	        jettyServer.join();
		} catch (Exception e) {
			throw new RuntimeException(e);
		}

	}
	
	public static String getJarPath() {
        final String classFileName = "/" + HadoopResultSaver.class.getName().replaceAll("\\.", "/") + ".class";
        final String classFilePath = HadoopResultSaver.class.getResource(classFileName).getPath();
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
			final String ap = req.getParameter("ap");
			LOG.info("dn: " + dn);
			LOG.info("ap: " + ap);
			resp.addHeader("Access-Control-Allow-Origin", "*");
			resp.addHeader("Access-Control-Allow-Headers", "Content-Type");
			resp.setContentType("application/json; charset=UTF-8");
			final PrintWriter out = resp.getWriter();
			switch (dn) {
			case "real":
				//out.println(realtimeGenerator.startToGetList());
				out.close();
				break;
			case "hist":
				out.println(historyReader.readLatestHistoryAsJson(ap));
				out.close();
				break;
			default:
				LOG.info("unexpected parameter dn: " + dn);
			}
		}
	}
}
