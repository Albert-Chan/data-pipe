package com.dataminer.job.submitter;

import java.util.Properties;

import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;

public class JobSubmissionEventTrigger {

	private Producer<String, String> producer;
	private String topic;

	public JobSubmissionEventTrigger(String brokers, String topic) {
		this.topic = topic;
		if (!brokers.isEmpty() && !topic.isEmpty()) {
			Properties props = new Properties();
			props.put("bootstrap.servers", brokers);
			props.put("acks", "all");
			props.put("retries", 0);
			props.put("batch.size", 16384);
			props.put("linger.ms", 1);
			props.put("buffer.memory", 33554432);
			props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
			props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");

			this.producer = new KafkaProducer<String, String>(props);
		}
	}

	public void trigger(String actionType, String module, String params) {
		if (null == producer) {
			return;
		}
		producer.send(new ProducerRecord<String, String>(topic, params, actionType + "," + module + "," + params),
				new Callback() {
					public void onCompletion(RecordMetadata metadata, Exception e) {
						if (e != null) {
							e.printStackTrace();
						}
					}
				});
	}

	public void close() {
		if (null != producer) {
			producer.close();
		}
	}

	public static void main(String[] args) {
		JobSubmissionEventTrigger trigger = new JobSubmissionEventTrigger(
				"192.168.111.191:9092,192.168.111.192:9092,192.168.111.193:9092", "jobSubmission");
		trigger.trigger("submit", "metro", "2017/02/15");
		trigger.close();
	}

}
