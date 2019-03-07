package com.blueskyarea.generator;

import java.lang.reflect.Type;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;

import com.blueskyarea.HadoopResultSaver;
import com.blueskyarea.entity.HadoopApp;
import com.google.gson.Gson;
import com.google.gson.reflect.TypeToken;

public class JobHistoryReader {
	public JobHistoryReader() {
	}
	
	public String readLatestHistoryAsJson(String ap, String from, String to) {
		Gson gson = new Gson();
		return gson.toJson(readLatestHistory(ap, from, to));
	}
	
	public List<HadoopApp> readLatestHistory(String ap, String from, String to) {
		try {
			Gson gson = new Gson();
			List<String> lines = Files.lines(
					Paths.get(HadoopResultSaver.historyFilePath),
					StandardCharsets.UTF_8).collect(Collectors.toList());
			Type listType = new TypeToken<List<HadoopApp>>(){}.getType();
			//return gson.fromJson(lines.get(0), listType);
			List<HadoopApp> list = gson.fromJson(lines.get(0), listType);
			return list.stream().filter(d -> d.name.equals(ap))
					.filter(d -> compareTime(from, d.startedTime) && compareTime(d.startedTime, to))
					.collect(Collectors.toList());
		} catch (Exception e) {
			e.printStackTrace();
			return new ArrayList<>();
		}
	}
	
	public boolean compareTime(String timeA, String timeB) {
		if (timeB.compareTo(timeA) >= 0)  {
			return true;
		}
		return false;
	}
}
