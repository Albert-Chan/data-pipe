package com.dataminer.monitor;

import java.util.Properties;

import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.log4j.Logger;

import com.dataminer.util.PassengerConfig;

public class AppEventTrigger {
	protected static final Logger LOG = Logger.getLogger(AppEventTrigger.class);

	private static final AppEventTrigger instance = new AppEventTrigger();
	private Producer<String, String> producer;
	private String topic;

	public static AppEventTrigger get() {
		return instance;
	}

	private AppEventTrigger() {
		PassengerConfig psgConf = PassengerConfig.getConfig();
		String brokers = psgConf.getKafkaBrokers();
		this.topic = psgConf.getMonitorTopicName();

		if (!brokers.isEmpty() && !this.topic.isEmpty()) {
			Properties props = new Properties();
			props.put("bootstrap.servers", brokers);
			props.put("acks", "all");
			props.put("retries", 0);
			props.put("batch.size", 1024);
			props.put("linger.ms", 0);
			props.put("buffer.memory", 10240);
			props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
			props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");

			this.producer = new KafkaProducer<String, String>(props);
		}

		// Add a shutdown hook to close the producer instance.
		try {
			Runtime.getRuntime().addShutdownHook(new Cleaner());
		} catch (IllegalStateException e) {
			// If the VM is already shutting down,
			// We do not need to register shutdownHook.
		}
	}

	private class Cleaner extends Thread {
		private Cleaner() {
			this.setContextClassLoader(null);
		}

		@Override
		public void run() {
			close();
		}
	}
	
	public void close() {
		if (null != producer) {
			producer.close();
		}
	}

	public void send(Message msg) {
		send(msg.getKey(), msg.getMessage());
	}

	private void send(String key, String message) {
		if (null == producer) {
			return;
		}
		producer.send(new ProducerRecord<String, String>(topic, key, message), new Callback() {
			public void onCompletion(RecordMetadata metadata, Exception e) {
				if (e != null) {
					LOG.error(e.getLocalizedMessage());
				}
			}
		});
	}

}
