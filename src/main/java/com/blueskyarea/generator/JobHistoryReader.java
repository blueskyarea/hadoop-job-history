package com.blueskyarea.generator;

import java.lang.reflect.Type;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.List;
import java.util.stream.Collectors;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.blueskyarea.config.HadoopResultSaverConfig;
import com.blueskyarea.entity.HadoopApp;
import com.blueskyarea.entity.HadoopHistory;
import com.google.gson.Gson;
import com.google.gson.reflect.TypeToken;

public class JobHistoryReader {
	private static final Logger LOG = LoggerFactory
			.getLogger("JobHistoryReader");
	private String historyFilePath;

	public JobHistoryReader(HadoopResultSaverConfig config) {
		this.historyFilePath = config.getHistoryFilePath();
	}

	public String readLatestHistoryAsJson(String ap, String from, String to) {
		return new Gson().toJson(readLatestHistory(ap, from, to));
	}

	protected HadoopHistory readLatestHistory(String ap, String from, String to) {
		try {
			List<String> lines = Files.lines(Paths.get(historyFilePath),
					StandardCharsets.UTF_8).collect(Collectors.toList());
			Type historyType = new TypeToken<HadoopHistory>() {}.getType();
			HadoopHistory hist = new Gson().fromJson(lines.get(0), historyType);
			List<HadoopApp> filteredHistories = hist.histories
					.stream()
					.filter(d -> d.name.equals(ap))
					.filter(d -> compareTime(from, d.startedTime)
							&& compareTime(d.startedTime, to))
					.collect(Collectors.toList());
			List<String> filteredApps = filteredHistories.stream()
					.map(d -> d.name).collect(Collectors.toList());
			return new HadoopHistory(filteredApps, filteredHistories);
		} catch (Exception e) {
			LOG.error("Failed to read latest history. path : "
					+ historyFilePath + "Returning empty history.");
			return new HadoopHistory();
		}
	}

	protected boolean compareTime(String timeA, String timeB) {
		if (timeB.compareTo(timeA) >= 0) {
			return true;
		}
		return false;
	}
}
