package com.dataminer.monitor;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;

import com.dataminer.monitor.Constants;

import kafka.consumer.ConsumerConfig;
import kafka.consumer.ConsumerIterator;
import kafka.consumer.KafkaStream;
import kafka.javaapi.consumer.ConsumerConnector;

public class AppEventWatcher implements Constants
{
	private final ConsumerConnector consumer;
	private final String topic;

	public AppEventWatcher(String topic, String group, String zkConnect) {
		this.topic = topic;

		Properties props = new Properties();
		props.put("zookeeper.connect", zkConnect);
		props.put("group.id", group);
		props.put("zookeeper.session.timeout.ms", "4000");
		props.put("zookeeper.sync.time.ms", "200");
		props.put("auto.commit.interval.ms", "1000");

		consumer = kafka.consumer.Consumer.createJavaConsumerConnector(new ConsumerConfig(props));
	}

	public void handle() {
		Map<String, Integer> topicCountMap = new HashMap<String, Integer>();
		topicCountMap.put(topic, new Integer(1));
		Map<String, List<KafkaStream<byte[], byte[]>>> consumerMap = consumer.createMessageStreams(topicCountMap);
		KafkaStream<byte[], byte[]> stream = consumerMap.get(topic).get(0);
		ConsumerIterator<byte[], byte[]> it = stream.iterator();
		while (it.hasNext()) {
			String message = new String(it.next().message());
			// add your event handler here...
			System.out.println(message);
		}
	}

	public static void main(String[] args) {
		AppEventWatcher watcher = new AppEventWatcher("jobStatus", "group4test",
				"192.168.111.191:2181,192.168.111.192:2181,192.168.111.193:2181");
		watcher.handle();
	}
}
