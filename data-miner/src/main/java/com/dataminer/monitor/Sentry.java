package com.dataminer.monitor;

import java.util.HashMap;
import java.util.Map;
import java.util.UUID;

import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.Printed;
import org.apache.kafka.streams.kstream.Produced;

public class Sentry<T> {
	private StreamsBuilder builder = new StreamsBuilder();
	private String brokers;

	public Sentry(String brokers) {
		this.brokers = brokers;
	}

	public static <T> Sentry<T> create(String brokers) {
		return new Sentry<T>(brokers);
	}

	public KStream<String, T> on(String fromTopic) {
		return builder.<String, T>stream(fromTopic);
	}

	public void print(KStream<String, T> stream) {
		stream.print(Printed.<String, T>toSysOut());
		start();
	}

	public void to(KStream<String, T> stream, final String topic) {
		stream.to(topic);
		start();
	}

	public void to(KStream<String, T> stream, final String topic, final Produced<String, T> produced) {
		stream.to(topic, produced);
		start();
	}

	public void start() {
		if (builder != null) {
			Map<String, Object> props = new HashMap<>();
			props.put(StreamsConfig.APPLICATION_ID_CONFIG, "monitoring_" + UUID.randomUUID());
			props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, this.brokers);
			props.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass());
			props.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass());
			StreamsConfig config = new StreamsConfig(props);
			KafkaStreams streams = new KafkaStreams(builder.build(), config);
			streams.start();
		}
	}

}
