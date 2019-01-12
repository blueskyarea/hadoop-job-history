package com.blueskyarea.thread;

import java.io.File;
import java.io.FileWriter;

import com.blueskyarea.HadoopResultSaver;

public class JobHistoryThread implements Runnable {
	
	public JobHistoryThread() {
	}
	
	@Override
    public void run() {
        while (true) {
            try {
            	Thread.sleep(5000);
            	System.out.println("test");
            	String result = HadoopResultSaver.realtimeGenerator.startToGetHistory();
				File file = new File(HadoopResultSaver.historyFilePath);
				FileWriter filewriter = new FileWriter(file);
				filewriter.write(result);
				filewriter.close();
            } catch(InterruptedException ignore) {
            } catch(Exception e) {
            	e.printStackTrace();
            }
        }
    }
}
