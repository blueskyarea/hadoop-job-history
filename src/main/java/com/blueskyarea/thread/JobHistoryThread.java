package com.blueskyarea.thread;

import java.io.File;
import java.io.FileWriter;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.blueskyarea.config.HadoopResultSaverConfig;
import com.blueskyarea.exception.HadoopJobHistorySaverRuntimeException;
import com.blueskyarea.generator.JobHistoryGenerator;

public class JobHistoryThread implements Runnable {
	
	private static final Logger LOG = LoggerFactory.getLogger("JobHistoryThread");
	private HadoopResultSaverConfig config;
	private String historyFilePath;
	private JobHistoryGenerator historyGenerator;
	
	public JobHistoryThread(HadoopResultSaverConfig config) {
		this.config = config;
		this.historyFilePath = config.getHistoryFilePath();
		this.historyGenerator = new JobHistoryGenerator(config);
	}
	
	@Override
    public void run() {
        while (true) {
            try (FileWriter filewriter = new FileWriter(new File(historyFilePath))) {
            	Thread.sleep(config.getIntervalGetHistory());
            	LOG.info("Getting history.");
				filewriter.write(historyGenerator.startToGetHistory());
            } catch(InterruptedException ignore) {
            } catch(Exception e) {
            	throw new HadoopJobHistorySaverRuntimeException(e.getMessage());
            }
        }
    }
}
