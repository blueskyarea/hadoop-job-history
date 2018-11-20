package com.blueskyarea.config;

import org.apache.commons.configuration.AbstractFileConfiguration;
import org.apache.commons.configuration.ConfigurationException;
import org.apache.commons.configuration.PropertiesConfiguration;

public class HadoopJobHistoryConfig {
	private static HadoopJobHistoryConfig instance;
	private AbstractFileConfiguration config;
	
	private HadoopJobHistoryConfig() throws ConfigurationException {
		this.config = new PropertiesConfiguration();
		this.config.setDelimiterParsingDisabled(true);
		this.config.load("hadoop-job-history.properties");
	}
	
	public synchronized static HadoopJobHistoryConfig getInstance() {
		try {
			if (instance == null) {
				instance = new HadoopJobHistoryConfig();
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
	
	public String getHadoopUser() {
		return config.getString("hadoop.user");
	}
}
