package com.blueskyarea.generator;

import java.lang.reflect.Type;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;

import com.blueskyarea.HadoopJobHistory;
import com.blueskyarea.entity.HadoopApp;
import com.google.gson.Gson;
import com.google.gson.reflect.TypeToken;

public class JobHistoryReader {
	public JobHistoryReader() {
	}
	
	public String readLatestHistoryAsJson() {
		Gson gson = new Gson();
		return gson.toJson(readLatestHistory());
	}
	
	public List<HadoopApp> readLatestHistory() {
		try {
			Gson gson = new Gson();
			List<String> lines = Files.lines(
					Paths.get(HadoopJobHistory.historyFilePath),
					StandardCharsets.UTF_8).collect(Collectors.toList());
			Type listType = new TypeToken<List<HadoopApp>>(){}.getType();
			return gson.fromJson(lines.get(0), listType);
		} catch (Exception e) {
			e.printStackTrace();
			return new ArrayList<>();
		}
	}
}
