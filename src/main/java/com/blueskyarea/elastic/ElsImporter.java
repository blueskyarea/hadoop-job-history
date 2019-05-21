package com.blueskyarea.elastic;

import java.io.IOException;

import org.elasticsearch.action.bulk.BulkRequest;
import org.elasticsearch.common.settings.Settings;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ElsImporter {
	private static final Logger LOG = LoggerFactory.getLogger("ElsImporter");
	
	public ElsImporter() {
	}
	
	public void execute(BulkRequest bulkRequest) throws IOException {
		LOG.info("starting");
		Settings settings = Settings.builder()
		        .put("cluster.name", "docker-cluster").build();
		//TransportClient client = new PreBuiltTransportClient(settings)
        //.addTransportAddress(new TransportAddress(InetAddress.getByName("172.40.0.1"), 9300));
		/*RestHighLevelClient client = new RestHighLevelClient(
		        RestClient.builder(
		                new HttpHost("172.40.0.1", 9200, "http")));*/
		
		//BulkResponse bulkResponse = (BulkResponse) client.bulk(bulkRequest);
		//if (bulkResponse.hasFailures()) { 
		//	LOG.error("Failed to save data into elastic.");
		//}
		
		LOG.info("ending");
		//client.close();
	}
}
