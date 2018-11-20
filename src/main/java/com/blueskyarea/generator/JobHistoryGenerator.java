package com.blueskyarea.generator;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.commons.lang3.time.DateFormatUtils;
import org.apache.log4j.Logger;

import com.blueskyarea.config.HadoopJobHistoryConfig;
import com.blueskyarea.entity.Hadoop;
import com.blueskyarea.entity.HadoopApp;
import com.google.api.client.http.GenericUrl;
import com.google.api.client.http.HttpRequest;
import com.google.api.client.http.HttpRequestFactory;
import com.google.api.client.http.HttpResponse;
import com.google.api.client.http.HttpTransport;
import com.google.api.client.http.javanet.NetHttpTransport;
import com.google.gson.Gson;
import com.google.gson.JsonSyntaxException;

public class JobHistoryGenerator {
	private static final Logger LOG = Logger.getLogger("JobHistoryGenerator");
	
	private HadoopJobHistoryConfig config;
	private String hadoopRestApi;
	
    public JobHistoryGenerator(HadoopJobHistoryConfig config) {
    	this.config = config;
    	this.hadoopRestApi = 
    			"http://" + config.getRmHost() + ":" + config.getRmPort() + "/ws/v1/cluster/apps?states=" + config.getHadoopStatus() + "&user=" + config.getHadoopUser();
    }
    
    public String startToGetList() throws JsonSyntaxException, IOException {
        HttpResponse response = getRequestHttpContents();
        return convertToView(response);
        //return "realtime";
    }
    
    protected HttpResponse getRequestHttpContents() throws IOException {
    	System.out.println("hadoopRestApi:" + hadoopRestApi);
    	LOG.info("hadoopRestApi:" + hadoopRestApi);
        //String apiUrl = "http://172.20.0.2:8088/ws/v1/cluster/apps?states=accepted,running,finished,failed,killed&user=foo";
        //String proxyHost = "proxy";
        //int proxyPort = 9502;
        HttpTransport transport = null;
        
        //Proxy proxy = new Proxy(Proxy.Type.HTTP, new InetSocketAddress(proxyHost, proxyPort));
        //transport = new NetHttpTransport.Builder().setProxy(proxy).build();
        transport = new NetHttpTransport.Builder().build();
        
        HttpRequestFactory factory = transport.createRequestFactory();
        GenericUrl genericUrl = new GenericUrl(hadoopRestApi);
        HttpRequest request = factory.buildGetRequest(genericUrl);
        return request.execute();
    }
    
    protected String convertToView(HttpResponse response) throws JsonSyntaxException, IOException {
        Gson gson = new Gson();
        List<HadoopApp> hadoopAppList = new ArrayList<HadoopApp>();

        Hadoop originalJson = gson.fromJson(response.parseAsString(), Hadoop.class);
        for (HadoopApp hadoopApp : originalJson.hadoopApps.hadoopApp) {
            HadoopApp app = new HadoopApp();
            app.startedTime = calcStartedTime(hadoopApp.startedTime);
            app.id = createIdWithTrackingUrl(hadoopApp);
            app.queue = hadoopApp.queue;
            app.name = hadoopApp.name;
            app.state = hadoopApp.state;
            app.finalStatus = hadoopApp.finalStatus;
            app.elapsedTime = calcElaspedTime(hadoopApp.elapsedTime);
            app.allocatedMB = hadoopApp.allocatedMB;
            app.allocatedVCores = hadoopApp.allocatedVCores;
            app.runningContainers = hadoopApp.runningContainers;
            app.queueUsagePercentage = hadoopApp.queueUsagePercentage;
            app.clusterUsagePercentage = hadoopApp.clusterUsagePercentage;
            hadoopAppList.add(app);
        }
        return gson.toJson(hadoopAppList);
    }
    
    protected String calcStartedTime(String startedTimeMin) {
        Long startedTime = Long.parseLong(startedTimeMin);
        return DateFormatUtils.format(startedTime, "yyyy/MM/dd HH:mm:ss");
    }
    
    protected String calcElaspedTime(String elapedTimeMin) {
        Long elapedTime = Long.parseLong(elapedTimeMin) - 32400000;
        return DateFormatUtils.format(elapedTime, "HH:mm:ss");
    }
    
    protected String createIdWithTrackingUrl(HadoopApp app) {
        return "<a href=\"" + app.trackingUrl + "\" target=\"_blank\">" + app.id + "</a>";
    }
}
