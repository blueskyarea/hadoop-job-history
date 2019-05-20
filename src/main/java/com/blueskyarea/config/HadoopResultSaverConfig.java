package com.blueskyarea.config;

import java.io.IOException;
import java.io.InputStream;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.blueskyarea.exception.HadoopJobHistorySaverRuntimeException;

public class HadoopResultSaverConfig {
	private static final Logger LOG = LoggerFactory
			.getLogger("HadoopResultSaverConfig");
	private static HadoopResultSaverConfig instance;
	private static String configFileName = "hadoop-result-saver.xml";

	private Properties properties = new Properties();
	private Map<String, String> propMap = new HashMap<>();

	public HadoopResultSaverConfig(String configFileName) {
		try {
			ClassLoader classLoader = Thread.currentThread().getContextClassLoader();
			InputStream istream = classLoader.getResourceAsStream(configFileName);
			properties.loadFromXML(istream);
			istream.close();
		} catch (IOException e) {
			throw new HadoopJobHistorySaverRuntimeException(e.getMessage());
		}

		for (Map.Entry<Object, Object> e : properties.entrySet()) {
			LOG.info(e.getKey().toString() + " " + e.getValue().toString());
			propMap.put(e.getKey().toString(), e.getValue().toString());
		}
	}

	public synchronized static HadoopResultSaverConfig getInstance() {
		if (instance == null) {
			instance = new HadoopResultSaverConfig(configFileName);
		}
		return instance;
	}
	
	public Boolean getAppWebEnable() {
		return Boolean.valueOf(propMap.get("app.web.enable"));
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
	
	public String getWelcomeFileName() {
		return propMap.get("welcome.file.name");
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

	public int getNumRetryHttpReq() {
		return Integer.valueOf(propMap.get("num.retry.http.req"));
	}

	public int getTimeoutConnectMil() {
		return Integer.valueOf(propMap.get("timeout.connect.mil"));
	}

	public int getTimeoutReadMil() {
		return Integer.valueOf(propMap.get("timeout.read.mil"));
	}

	public int getIntervalGetHistory() {
		return Integer.valueOf(propMap.get("interval.get.history.millisec"));
	}

	public int getDaysToKeepHistory() {
		return Integer.valueOf(propMap.get("days.to.keep.history"));
	}
}
