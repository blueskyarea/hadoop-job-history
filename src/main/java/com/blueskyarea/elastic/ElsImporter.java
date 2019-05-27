package com.blueskyarea.elastic;

import java.io.IOException;

import org.apache.http.HttpHost;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.index.IndexResponse;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.common.xcontent.XContentType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.blueskyarea.config.HadoopResultSaverConfig;

public class ElsImporter {
	private static final Logger LOG = LoggerFactory.getLogger("ElsImporter");
	private RestHighLevelClient client;
	private String elasticIndex;

	public ElsImporter() {
		this.client = new RestHighLevelClient(
				RestClient.builder(new HttpHost(HadoopResultSaverConfig
						.getInstance().getElasticHost(),
						HadoopResultSaverConfig.getInstance().getElasticPort(),
						"http")));
		this.elasticIndex = HadoopResultSaverConfig.getInstance().getElasticIndex();
	}

	public void execute(String jsonString) throws IOException {
		LOG.info("starting to elastic");
		IndexRequest request = new IndexRequest(this.elasticIndex);
		request.source(jsonString, XContentType.JSON);
		IndexResponse indexResponse = client.index(request,
				RequestOptions.DEFAULT);
		LOG.info("created index with id : " + indexResponse.getId());
	}
}
