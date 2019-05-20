package com.blueskyarea.generator;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.lang.reflect.Type;
import java.net.InetSocketAddress;
import java.net.NoRouteToHostException;
import java.net.Proxy;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.text.SimpleDateFormat;
import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

import org.apache.commons.lang3.time.DateFormatUtils;
import org.elasticsearch.action.bulk.BulkRequest;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.common.xcontent.XContentType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.blueskyarea.config.HadoopResultSaverConfig;
import com.blueskyarea.elastic.ElsImporter;
import com.blueskyarea.entity.Hadoop;
import com.blueskyarea.entity.HadoopApp;
import com.blueskyarea.entity.HadoopHistory;
import com.blueskyarea.exception.HadoopJobHistorySaverException;
import com.blueskyarea.exception.HadoopJobHistorySaverRuntimeException;
import com.google.api.client.http.GenericUrl;
import com.google.api.client.http.HttpRequest;
import com.google.api.client.http.HttpRequestFactory;
import com.google.api.client.http.HttpResponse;
import com.google.api.client.http.HttpTransport;
import com.google.api.client.http.javanet.NetHttpTransport;
import com.google.gson.Gson;
import com.google.gson.JsonSyntaxException;
import com.google.gson.reflect.TypeToken;

public class JobHistoryGenerator {
	private static final Logger LOG = LoggerFactory.getLogger("JobHistoryGenerator");

	private HadoopResultSaverConfig config;
	private String hadoopRestApi;
	private String historyFilePath;
	private SimpleDateFormat dfForStartTime = new SimpleDateFormat("yyyy/MM/dd HH:mm:ss");

	public JobHistoryGenerator(HadoopResultSaverConfig config) {
		this.config = config;
		this.historyFilePath = config.getHistoryFilePath();
		this.hadoopRestApi = "http://" + config.getRmHost() + ":"
				+ config.getRmPort() + "/ws/v1/cluster/apps?states="
				+ config.getHadoopStatusHistory() + "&user="
				+ config.getHadoopUser();
	}

	public String startToGetHistory() throws IOException, HadoopJobHistorySaverException {
		HttpResponse response = getRequestHttpContents();
		List<HadoopApp> latestHistory = readLatestHistory();
		return convertToView(response, latestHistory);
	}

	protected HttpResponse getRequestHttpContents() throws IOException {
		LOG.info("Hadoop REST API:" + hadoopRestApi);
		Boolean proxyUse = config.getProxyUse();
		HttpTransport transport = null;
		if (proxyUse) {
			Proxy proxy = new Proxy(Proxy.Type.HTTP, new InetSocketAddress(config.getProxyHost(), config.getProxyPort()));
			transport = new NetHttpTransport.Builder().setProxy(proxy).build();
		} else {
			transport = new NetHttpTransport.Builder().build();
		}

		HttpRequestFactory factory = transport.createRequestFactory();
		GenericUrl genericUrl = new GenericUrl(hadoopRestApi);
		HttpRequest request = factory.buildGetRequest(genericUrl)
				.setNumberOfRetries(config.getNumRetryHttpReq())
				.setConnectTimeout(config.getTimeoutConnectMil())
				.setReadTimeout(config.getTimeoutReadMil());
		try {
			return request.execute();
		} catch (NoRouteToHostException e) {
			throw new HadoopJobHistorySaverRuntimeException(e.getMessage());
		}
	}
	
	protected List<HadoopApp> readLatestHistory() {
		try {
			// if history is not existing just return empty list
			File file = new File(historyFilePath);
			if (!file.exists()) {
				return new ArrayList<>();
			}
			
			List<String> lines = Files.lines(
					Paths.get(historyFilePath),
					StandardCharsets.UTF_8).collect(Collectors.toList());
			if (lines.isEmpty()) {
				return new ArrayList<>();
			}
			
			Type historyType = new TypeToken<HadoopHistory>(){}.getType();
			HadoopHistory hist = new Gson().fromJson(lines.get(0), historyType);
			return hist.histories;
		} catch (Exception e) {
			LOG.error("Failed to read latest history. path : "
					+ historyFilePath + "Returning empty history.", e.getMessage());
			return new ArrayList<>();
		}
	}

	protected String convertToView(HttpResponse response, List<HadoopApp> latestHistory)
			throws IOException {
		Gson gson = new Gson();
		
		// filter by epoc time
		long epochToKeepHistory = Instant.now().minus(config.getDaysToKeepHistory(), ChronoUnit.DAYS).toEpochMilli();
		List<HadoopApp> filteredLatestHistory =
				latestHistory.stream().filter(hist -> {
					long epoch = 0;
					try {
						epoch = dfForStartTime.parse(hist.startedTime).getTime();
					} catch (Exception e) {
						throw new HadoopJobHistorySaverRuntimeException(e.getMessage());
					}
					return epoch > epochToKeepHistory;
				}).collect(Collectors.toList());
		
		List<String> idList = new ArrayList<>();
		if (filteredLatestHistory.size() > 0) {
			filteredLatestHistory.forEach(history -> {
				idList.add(history.id);
			});
		}
		
		Hadoop originalJson = gson.fromJson(response.parseAsString(),
				Hadoop.class);
		
		// if response does not have HadoopApps, just return latest history.
		if (originalJson == null || originalJson.hadoopApps == null) {
			return gson.toJson(latestHistory);
		}
		
		// elastic
		BulkRequest bulkRequest = new BulkRequest();
		
		for (HadoopApp hadoopApp : originalJson.hadoopApps.hadoopApp) {
			HadoopApp app = new HadoopApp();
			app.id = createIdWithTrackingUrl(hadoopApp);
			if (idList.contains(app.id)) {
				LOG.info("This id is already saved: " + hadoopApp.id);
				continue;
			}
			LOG.info("This id is new: " + hadoopApp.id);
			app.startedTime = calcStartedTime(hadoopApp.startedTime);
			app.finishedTime = calcStartedTime(hadoopApp.finishedTime);
			app.queue = hadoopApp.queue;
			app.name = hadoopApp.name;
			app.state = hadoopApp.state;
			app.finalStatus = hadoopApp.finalStatus;
			app.elapsedTime = calcElaspedTime(hadoopApp.elapsedTime);
			app.memorySeconds = hadoopApp.memorySeconds;
			app.vcoreSeconds = hadoopApp.vcoreSeconds;
			LOG.info("Long.parseLong(hadoopApp.memorySeconds)" + Long.parseLong(hadoopApp.memorySeconds));
			LOG.info("Long.parseLong(hadoopApp.vcoreSeconds)" + Long.parseLong(hadoopApp.vcoreSeconds));
			//app.allocatedMBPerSeconds = String.valueOf(Long.parseLong(hadoopApp.memorySeconds) / Long.parseLong(DateFormatUtils.format(elapedTime, "ss")));
			//app.allocatedVCoresPerSeconds = String.valueOf(Long.parseLong(hadoopApp.vcoreSeconds) / Long.parseLong(DateFormatUtils.format(elapedTime, "ss")));
			app.allocatedMB = hadoopApp.allocatedMB;
			app.allocatedVCores = hadoopApp.allocatedVCores;
			app.runningContainers = hadoopApp.runningContainers;
			app.queueUsagePercentage = hadoopApp.queueUsagePercentage;
			app.clusterUsagePercentage = hadoopApp.clusterUsagePercentage;
			filteredLatestHistory.add(app);
			
			// elastic
			IndexRequest request = new IndexRequest("posts");
			String jsonString = gson.toJson(app);
			request.source(jsonString, XContentType.JSON);
			bulkRequest.add(request);
		}
		
		// elastic
		new ElsImporter().execute(bulkRequest);
		
		// should create name list after updated history.
		Set<String> nameSet = new HashSet<>();
		filteredLatestHistory.forEach(history -> {
			nameSet.add(history.name);
		});
		HadoopHistory hist = new HadoopHistory(new ArrayList<>(nameSet), filteredLatestHistory);
		return gson.toJson(hist);
	}
	
	protected void saveNameList(Set<String> nameSet) throws IOException {
		BufferedWriter out = new BufferedWriter(new FileWriter("/tmp/apps.txt"));
		Iterator<String> it = nameSet.iterator();
		while(it.hasNext()) {
		    out.write(it.next());
		    out.newLine();
		}
		out.close();
	}

	protected String calcStartedTime(String startedTimeMin) {
		Long startedTime = Long.parseLong(startedTimeMin);
		return DateFormatUtils.format(startedTime, "yyyy/MM/dd HH:mm:ss");
	}

	protected String calcElaspedTime(String elapedTimeMin) {
		Long elapedTime = Long.parseLong(elapedTimeMin) - 32400000;
		return DateFormatUtils.format(elapedTime, "HH:mm:ss");
	}

	protected String createIdWithTrackingUrl(HadoopApp app) {
		return "<a href=\"" + app.trackingUrl + "\" target=\"_blank\">"
				+ app.id + "</a>";
	}
}
