package com.blueskyarea.entity;

import java.util.ArrayList;
import java.util.List;

import com.google.gson.annotations.SerializedName;

public class HadoopHistory {
	@SerializedName("apps")
	public List<String> apps;
	
	@SerializedName("histories")
	public List<HadoopApp> histories;
	
	public HadoopHistory(){
		this.apps = new ArrayList<String>();
		this.histories = new ArrayList<HadoopApp>();
	}
	
	public HadoopHistory(List<String> apps, List<HadoopApp> histories) {
		this.apps = apps;
		this.histories = histories;
	}
}
