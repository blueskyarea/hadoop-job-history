package com.blueskyarea.thread;

import java.io.File;
import java.io.FileWriter;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.blueskyarea.HadoopResultSaver;
import com.blueskyarea.config.HadoopResultSaverConfig2;
import com.blueskyarea.exception.HadoopResultSaverException;

public class JobHistoryThread implements Runnable {
	
	private static final Logger LOG = LoggerFactory.getLogger("JobHistoryThread");
	private HadoopResultSaverConfig2 config;
	
	public JobHistoryThread(HadoopResultSaverConfig2 config2) {
		this.config = config2;
	}
	
	@Override
    public void run() {
        while (true) {
            try {
            	Thread.sleep(config.getIntervalGetHistory());
            	System.out.println("test");
            	String result = HadoopResultSaver.realtimeGenerator.startToGetHistory();
				File file = new File(HadoopResultSaver.historyFilePath);
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
