package com.blueskyarea.generator;

import java.lang.reflect.Type;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;

import com.blueskyarea.HadoopJobHistorySaver;
import com.blueskyarea.config.HadoopResultSaverConfig;
import com.blueskyarea.entity.HadoopApp;
import com.blueskyarea.entity.HadoopHistory;
import com.blueskyarea.exception.HadoopJobHistorySaverException;
import com.google.gson.Gson;
import com.google.gson.reflect.TypeToken;

public class JobHistoryReader {
	private String historyFilePath;
	
	public JobHistoryReader(HadoopResultSaverConfig config) {
		this.historyFilePath = config.getHistoryFilePath();
	}
	
	public String readLatestHistoryAsJson(String ap, String from, String to) {
		Gson gson = new Gson();
		return gson.toJson(readLatestHistory(ap, from, to));
	}
	
	public HadoopHistory readLatestHistory(String ap, String from, String to) {
		try {
			Gson gson = new Gson();
			List<String> lines = Files.lines(
					Paths.get(historyFilePath),
					StandardCharsets.UTF_8).collect(Collectors.toList());
			Type listType = new TypeToken<List<HadoopApp>>(){}.getType();
			//return gson.fromJson(lines.get(0), listType);
			//List<HadoopApp> list = gson.fromJson(lines.get(0), listType);
			Type historyType = new TypeToken<HadoopHistory>(){}.getType();
			HadoopHistory hist = gson.fromJson(lines.get(0), historyType);
			System.out.println("--------------");
			System.out.println(ap);
			System.out.println("--------------");
			List<HadoopApp> filteredHistories = hist.histories.stream().filter(d -> d.name.equals("org.apache.spark.examples.SparkPi"))
					.filter(d -> compareTime(from, d.startedTime) && compareTime(d.startedTime, to))
					.collect(Collectors.toList());
			
			return new HadoopHistory(hist.apps, filteredHistories);
			
			//return list.stream().filter(d -> d.name.equals(ap))
			//		.filter(d -> compareTime(from, d.startedTime) && compareTime(d.startedTime, to))
			//		.collect(Collectors.toList());
		} catch (Exception e) {
			e.printStackTrace();
			return new HadoopHistory();
		}
	}
	
	public boolean compareTime(String timeA, String timeB) {
		if (timeB.compareTo(timeA) >= 0)  {
			return true;
		}
		return false;
	}
}
