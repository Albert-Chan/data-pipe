package com.dataminer.acquisitor;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;

import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class PostHandler {
	private static Logger logger = LoggerFactory.getLogger(PostHandler.class);

	private Producer<String, String> producer;
	private String topic;

	public PostHandler(Producer<String, String> producer, String topic) {
		this.producer = producer;
		this.topic = topic;
	}

	public void handle(InputStream inputStream) {
		try {
			BufferedReader reader = new BufferedReader(new InputStreamReader(inputStream, "GBK"));
			String strLine = reader.readLine();
			logger.debug("Sampled line:\t" + strLine);
			while (strLine != null) {
				producer.send(new ProducerRecord<>(topic, String.valueOf(System.currentTimeMillis()), strLine),
						((recordMetadata, e) -> {
							if (e != null) {
								logger.warn(e.getMessage());
							}
							if (recordMetadata != null) {
								logger.debug("We just sent a record into partition: " + recordMetadata.partition()
										+ ", at offset: " + recordMetadata.offset() + ".");
							}
						}));
				strLine = reader.readLine();
			}
			reader.close();
		} catch (IOException ioe) {
			logger.warn(ioe.getMessage());
		}
	}
}
