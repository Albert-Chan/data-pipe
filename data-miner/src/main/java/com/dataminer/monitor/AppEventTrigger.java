package com.dataminer.monitor;

import java.util.Properties;

import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.log4j.Logger;

import com.dataminer.configuration.ConfigManager;

@Deprecated
/**
 * As we switched to log4j's Kafka appender, we don't use this trigger any more.
 *
 */
public class AppEventTrigger {
	protected static final Logger LOG = Logger.getLogger(AppEventTrigger.class);

	private static final AppEventTrigger instance = new AppEventTrigger();
	private Producer<String, String> producer;
	private String topic;

	public static AppEventTrigger get() {
		return instance;
	}

	private AppEventTrigger() {
		ConfigManager conf = ConfigManager.getConfig();
		String brokers = conf.getProperty(Constants.KAFKA_BROKERS);
		this.topic = conf.getProperty(Constants.KAFKA_MONITOR_TOPIC);

		if (!isEmptyString(brokers) && !isEmptyString(this.topic)) {
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

	private boolean isEmptyString(String testString) {
		return (null == testString) || testString.isEmpty();
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

	public void send(String key, String message) {
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
