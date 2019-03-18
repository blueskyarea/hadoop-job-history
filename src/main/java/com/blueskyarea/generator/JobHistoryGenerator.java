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
import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;

import org.apache.commons.lang3.time.DateFormatUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.blueskyarea.HadoopResultSaver;
import com.blueskyarea.config.HadoopResultSaverConfig;
import com.blueskyarea.config.HadoopResultSaverConfig2;
import com.blueskyarea.entity.Hadoop;
import com.blueskyarea.entity.HadoopApp;
import com.blueskyarea.exception.HadoopResultSaverException;
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

	private HadoopResultSaverConfig2 config;
	private String hadoopRestApi;

	public JobHistoryGenerator(HadoopResultSaverConfig2 config) {
		this.config = config;
	}

	public String startToGetList() throws JsonSyntaxException, IOException, HadoopResultSaverException {
		this.hadoopRestApi = "http://" + config.getRmHost() + ":"
				+ config.getRmPort() + "/ws/v1/cluster/apps?states="
				+ config.getHadoopStatus() + "&user=" + config.getHadoopUser();
		HttpResponse response = getRequestHttpContents();
		return convertToView(response, new ArrayList<>());
	}

	public String startToGetHistory() throws IOException, HadoopResultSaverException {
		this.hadoopRestApi = "http://" + config.getRmHost() + ":"
				+ config.getRmPort() + "/ws/v1/cluster/apps?states="
				+ config.getHadoopStatusHistory() + "&user="
				+ config.getHadoopUser();
		HttpResponse response = getRequestHttpContents();
		List<HadoopApp> latestHistory = readLatestHistory();
		return convertToView(response, latestHistory);
	}

	protected HttpResponse getRequestHttpContents() throws IOException, HadoopResultSaverException {
		LOG.info("hadoopRestApi:" + hadoopRestApi);
		Boolean proxyUse = config.getProxyUse();
		HttpTransport transport = null;
		if (proxyUse) {
			String proxyHost = config.getProxyHost();
	        int proxyPort = config.getProxyPort();
			Proxy proxy = new Proxy(Proxy.Type.HTTP, new InetSocketAddress(proxyHost, proxyPort));
			transport = new NetHttpTransport.Builder().setProxy(proxy).build();
		} else {
			transport = new NetHttpTransport.Builder().build();
		}

		HttpRequestFactory factory = transport.createRequestFactory();
		GenericUrl genericUrl = new GenericUrl(hadoopRestApi);
		HttpRequest request = factory.buildGetRequest(genericUrl);
		try {
			return request.execute();
		} catch (NoRouteToHostException e) {
			throw new HadoopResultSaverException(e.getMessage());
		}
	}
	
	protected List<HadoopApp> readLatestHistory() {
		try {
			// if history is not existing just return empty list
			File file = new File(HadoopResultSaver.historyFilePath);
			if (!file.exists()) {
				return new ArrayList<>();
			}
			
			List<String> lines = Files.lines(
					Paths.get(HadoopResultSaver.historyFilePath),
					StandardCharsets.UTF_8).collect(Collectors.toList());
			Type listType = new TypeToken<List<HadoopApp>>(){}.getType();
			if (lines.isEmpty()) {
				return new ArrayList<>();
			}
			
			Gson gson = new Gson();
			return gson.fromJson(lines.get(0), listType);
		} catch (Exception e) {
			e.printStackTrace();
			return new ArrayList<>();
		}
	}

	protected String readLatestHistory2() {
		try {
			Gson gson = new Gson();
			List<String> lines = Files.lines(
					Paths.get(HadoopResultSaver.historyFilePath),
					StandardCharsets.UTF_8).collect(Collectors.toList());
			Type listType = new TypeToken<List<HadoopApp>>(){}.getType();
			List<HadoopApp> list = gson.fromJson(lines.get(0), listType);
			//Hadoop originalJson = gson.fromJson(reader, Hadoop.class);
			if (list.get(0) != null) {
				System.out.println(list.get(0).id);
				return list.get(0).id;
			}
			return "0";
		} catch (Exception e) {
			e.printStackTrace();
			return "0";
		}
	}

	protected String convertToView(HttpResponse response, List<HadoopApp> latestHistory)
			throws JsonSyntaxException, IOException {
		Gson gson = new Gson();
		//String latestId = "0";
		
		// filter by epoctime
		long epochToKeepHistory = Instant.now().minus(config.getDaysToKeepHistory(), ChronoUnit.DAYS).toEpochMilli();
		List<HadoopApp> filteredLatestHistory =
				latestHistory.stream().filter(hist -> Long.valueOf(hist.startedTime) > epochToKeepHistory).collect(Collectors.toList());
		
		List<String> idList = new ArrayList<>();
		Set<String> nameSet = new HashSet<>();
		if (filteredLatestHistory.size() > 0) {
			filteredLatestHistory.forEach(history -> {
				idList.add(history.id);
				nameSet.add(history.name);
			});
			//latestId = latestHistory.get(latestHistory.size() - 1).id;
		}
		
		// save name set
		saveNameList(nameSet);
		
		Hadoop originalJson = gson.fromJson(response.parseAsString(),
				Hadoop.class);
		
		if (originalJson == null || originalJson.hadoopApps == null) {
			return gson.toJson(latestHistory);
		}
		
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
			Long elapedTime = Long.parseLong(hadoopApp.elapsedTime) - 32400000;
			app.elapsedTime = calcElaspedTime(elapedTime);
			app.memorySeconds = hadoopApp.memorySeconds;
			app.vcoreSeconds = hadoopApp.vcoreSeconds;
			LOG.info("elapsedTime: " + elapedTime);
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
		}
		
		return gson.toJson(filteredLatestHistory);
	}
	
	protected void saveNameList(Set<String> nameSet) throws IOException {
		BufferedWriter out = new BufferedWriter(new FileWriter(HadoopResultSaver.thisJarDirPath + "/apps.txt"));
		Iterator<String> it = nameSet.iterator();
		System.out.println(nameSet.size());
		nameSet.forEach(System.out::println);
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
	
	protected String calcElaspedTime(Long elapedTime) {
		return DateFormatUtils.format(elapedTime, "HH:mm:ss");
	}

	/*protected String calcElaspedTime(String elapedTimeMin) {
		Long elapedTime = Long.parseLong(elapedTimeMin) - 32400000;
		return DateFormatUtils.format(elapedTime, "HH:mm:ss");
	}*/

	protected String createIdWithTrackingUrl(HadoopApp app) {
		return "<a href=\"" + app.trackingUrl + "\" target=\"_blank\">"
				+ app.id + "</a>";
	}
}
