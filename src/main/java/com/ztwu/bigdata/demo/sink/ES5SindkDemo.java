package com.ztwu.bigdata.demo.sink;

import org.apache.flink.api.common.functions.RuntimeContext;
import org.apache.flink.streaming.connectors.elasticsearch.ElasticsearchSinkFunction;
import org.apache.flink.streaming.connectors.elasticsearch.RequestIndexer;
import org.apache.flink.streaming.connectors.elasticsearch5.ElasticsearchSink;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.client.Requests;

import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.UnknownHostException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class ES5SindkDemo {
	public static void main(String[] args) throws UnknownHostException {
		Map<String, String> config = new HashMap<>();
		config.put("cluster.name", "my-cluster-name");
// This instructs the sink to emit after every element, otherwise they would be buffered
		config.put("bulk.flush.max.actions", "1");

		List<InetSocketAddress> transportAddresses = new ArrayList<>();
		transportAddresses.add(new InetSocketAddress(InetAddress.getByName("127.0.0.1"), 9300));
		transportAddresses.add(new InetSocketAddress(InetAddress.getByName("10.2.3.1"), 9300));

		new ElasticsearchSink<>(config, transportAddresses, new ElasticsearchSinkFunction<String>() {
			public IndexRequest createIndexRequest(String element) {
				Map<String, String> json = new HashMap<>();
				json.put("data", element);

				return Requests.indexRequest()
						.index("my-index")
						.type("my-type")
						.source(json);
			}

			@Override
			public void process(String element, RuntimeContext ctx, RequestIndexer indexer) {
				indexer.add(createIndexRequest(element));
			}
		});
	}
}
