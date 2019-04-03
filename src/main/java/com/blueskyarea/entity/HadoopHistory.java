package com.blueskyarea.entity;

import java.util.List;

import com.google.gson.annotations.SerializedName;

public class HadoopHistory {
	@SerializedName("apps")
	public String apps;
	
	@SerializedName("histories")
	public List<HadoopApp> histories;
	
	public HadoopHistory(){
	}
	
	public HadoopHistory(String apps, List<HadoopApp> histories) {
		this.apps = apps;
		this.histories = histories;
	}
}
