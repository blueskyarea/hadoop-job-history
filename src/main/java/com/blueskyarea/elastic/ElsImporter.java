package com.blueskyarea.elastic;

import java.io.IOException;

import org.apache.http.HttpHost;
import org.elasticsearch.action.bulk.BulkRequest;
import org.elasticsearch.action.bulk.BulkResponse;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.client.RestHighLevelClient;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ElsImporter {
	private static final Logger LOG = LoggerFactory.getLogger("ElsImporter");
	
	public ElsImporter() {
	}
	
	public void execute(BulkRequest bulkRequest) throws IOException {
		RestHighLevelClient client = new RestHighLevelClient(
		        RestClient.builder(
		                new HttpHost("172.40.0.1", 9200, "http")));
		
		BulkResponse bulkResponse = client.bulk(bulkRequest, RequestOptions.DEFAULT);
		if (bulkResponse.hasFailures()) { 
			LOG.error("Failed to save data into elastic.");
		}
		client.close();
	}
}
