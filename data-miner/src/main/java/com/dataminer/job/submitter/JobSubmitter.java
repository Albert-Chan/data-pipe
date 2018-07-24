package com.dataminer.job.submitter;

import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import kafka.consumer.ConsumerConfig;
import kafka.consumer.ConsumerIterator;
import kafka.consumer.KafkaStream;
import kafka.javaapi.consumer.ConsumerConnector;

/**
 * Job submitter receives the job launch event, and submits jobs to Spark
 * cluster.
 *
 */
public class JobSubmitter {
	private static final String ACTION_SUBMIT = "submit";
	private static final String ACTION_TERMINATE = "terminate";

	private static Logger logger = LoggerFactory.getLogger(JobSubmitter.class);

	private final ConsumerConnector consumer;
	private final String topic;

	private static String pfRoot = System.getenv("PF_ROOT");

	public JobSubmitter(String topic, String group, String zkConnect) {
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
		topicCountMap.put(topic, Integer.valueOf(1));
		Map<String, List<KafkaStream<byte[], byte[]>>> consumerMap = consumer.createMessageStreams(topicCountMap);
		KafkaStream<byte[], byte[]> stream = consumerMap.get(topic).get(0);
		ConsumerIterator<byte[], byte[]> it = stream.iterator();
		while (it.hasNext()) {
			String message = new String(it.next().message());
			String[] fragments = message.split(",", 2);
			if (fragments.length <= 0) {
				logger.warn(String.format("Invalid message: %s. Unable to parse the action type.", message));
				continue;
			}
			String action = fragments[0];
			if (action.equalsIgnoreCase(ACTION_SUBMIT)) {
				String[] args = fragments[1].split(",");
				if (args.length != 2) {
					logger.warn(String.format("Invalid arguments '%s' for job submission.", fragments[1]));
					continue;
				}
				JobSubmissionEvent event = new JobSubmissionEvent(args[0], args[1]);
				event.handle();
			}
			if (action.equalsIgnoreCase(ACTION_TERMINATE)) {
				String appId = fragments[1];
				JobTerminationEvent event = new JobTerminationEvent(appId);
				event.handle();
			}

		}
	}

	class JobSubmissionEvent {
		String module;
		String params;

		public JobSubmissionEvent(String module, String params) {
			this.module = module;
			this.params = params;
		}

		public void handle() {
			try {
				String commandLine = new StringBuilder().append(pfRoot).append("bin/").append(module).append(".sh")
						.append(" ").append(params).toString();
				logger.info(commandLine);
				Runtime.getRuntime().exec(commandLine);
			} catch (IOException e) {
				logger.warn(e.getMessage());
			}
		}
	}

	class JobTerminationEvent {
		String appId;

		public JobTerminationEvent(String appId) {
			this.appId = appId;
		}

		public void handle() {
			try {
				String commandLine = new StringBuilder().append("yarn application -kill ").append(appId).toString();
				logger.info(commandLine);
				Runtime.getRuntime().exec(commandLine);
			} catch (IOException e) {
				logger.warn(e.getMessage());
			}
		}
	}

}