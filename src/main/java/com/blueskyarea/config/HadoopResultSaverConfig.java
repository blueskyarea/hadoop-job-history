package com.blueskyarea.config;

import org.apache.commons.configuration.AbstractFileConfiguration;
import org.apache.commons.configuration.ConfigurationException;
import org.apache.commons.configuration.PropertiesConfiguration;

public class HadoopResultSaverConfig {
	private static HadoopResultSaverConfig instance;
	private AbstractFileConfiguration config;
	
	private HadoopResultSaverConfig() throws ConfigurationException {
		this.config = new PropertiesConfiguration();
		this.config.setDelimiterParsingDisabled(true);
		this.config.load("hadoop-result-saver.properties");
	}
	
	public synchronized static HadoopResultSaverConfig getInstance() {
		try {
			if (instance == null) {
				instance = new HadoopResultSaverConfig();
			}
			return instance;
		} catch (Exception e) {
			throw new RuntimeException(e);
		}
	}
	
	public int getAppPort() {
		return config.getInt("app.port");
	}
	
	public String getRmHost() {
		return config.getString("hadoop.rm.hostname");
	}
	
	public int getRmPort() {
		return config.getInt("hadoop.rm.port");
	}
	
	public String getHadoopStatus() {
		return config.getString("hadoop.status");
	}
	
	public String getHadoopStatusHistory() {
		return config.getString("hadoop.status.history");
	}
	
	public String getHadoopUser() {
		return config.getString("hadoop.user");
	}
}
