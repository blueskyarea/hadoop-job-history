package com.blueskyarea.thread;

import java.io.File;
import java.io.FileWriter;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.blueskyarea.HadoopJobHistorySaver;
import com.blueskyarea.config.HadoopResultSaverConfig;
import com.blueskyarea.exception.HadoopResultSaverException;
import com.blueskyarea.generator.JobHistoryGenerator;

public class JobHistoryThread implements Runnable {
	
	private static final Logger LOG = LoggerFactory.getLogger("JobHistoryThread");
	private JobHistoryGenerator historyGenerator;
	private HadoopResultSaverConfig config;
	private String historyFilePath;
	
	public JobHistoryThread(HadoopResultSaverConfig config) {
		this.config = config;
		this.historyGenerator = new JobHistoryGenerator(config);
		this.historyFilePath = config.getHistoryFilePath();
	}
	
	@Override
    public void run() {
        while (true) {
            try {
            	Thread.sleep(config.getIntervalGetHistory());
            	LOG.info("Getting history.");
            	String result = historyGenerator.startToGetHistory();
				File file = new File(historyFilePath);
				FileWriter filewriter = new FileWriter(file);
				filewriter.write(result);
				filewriter.close();
            } catch(InterruptedException ignore) {
            } catch(HadoopResultSaverException e) {
            	LOG.warn("Failed to get history. " + e);
            } catch(Exception e) {
            	e.printStackTrace();
            }
        }
    }
}
