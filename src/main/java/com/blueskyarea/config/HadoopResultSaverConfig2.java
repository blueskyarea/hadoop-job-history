package com.blueskyarea.config;

import java.io.IOException;
import java.io.InputStream;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

public class HadoopResultSaverConfig2 {
	private static HadoopResultSaverConfig2 instance;
	
	Properties properties = new Properties();
	String strpass = "hadoop-result-saver.xml";
	Map<String, String> propMap = new HashMap<>();
	
	public HadoopResultSaverConfig2() {
	    try {
	    	ClassLoader classLoader = Thread.currentThread().getContextClassLoader();
	    	InputStream istream = classLoader.getResourceAsStream(strpass);
	        //InputStream istream = new FileInputStream(strpass);
	        properties.loadFromXML(istream);
	        istream.close();
	    } catch (IOException e) {
	        e.printStackTrace();
	    }
	    
        for(Map.Entry<Object, Object> e : properties.entrySet()) {
        	System.out.println(e.getKey().toString() + " " + e.getValue().toString());
            propMap.put(e.getKey().toString(), e.getValue().toString());
        }
	}
	
	public synchronized static HadoopResultSaverConfig2 getInstance() {
		if (instance == null) {
			instance = new HadoopResultSaverConfig2();
		}
		return instance;
	}
	
	public int getAppPort() {
		return Integer.valueOf(propMap.get("app.port"));
	}
	
	public String getRmHost() {
		return propMap.get("hadoop.rm.hostname");
	}
	
	public int getRmPort() {
		return Integer.valueOf(propMap.get("hadoop.rm.port"));
	}
	
	public String getHadoopStatus() {
		return propMap.get("hadoop.status");
	}
	
	public String getHadoopUser() {
		return propMap.get("hadoop.user");
	}
	
	public String getHadoopStatusHistory() {
		return propMap.get("hadoop.status.history");
	}
	
	public String getHistoryFilePath() {
		return propMap.get("history.file.path");
	}
	
	public Boolean getProxyUse() {
		return Boolean.valueOf(propMap.get("proxy.use"));
	}
	
	public String getProxyHost() {
		return propMap.get("proxy.host");
	}
	
	public int getProxyPort() {
		return Integer.valueOf(propMap.get("proxy.port"));
	}
	
	public int getIntervalGetHistory() {
		return Integer.valueOf(propMap.get("interval.get.history.millisec"));
	}
	
	public int getDaysToKeepHistory() {
		return Integer.valueOf(propMap.get("days.to.keep.history"));
	}
}
