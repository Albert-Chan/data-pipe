package com.dataminer.acquisitor;

import java.io.BufferedReader;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.Properties;
import java.util.Scanner;
import java.util.Timer;
import java.util.TimerTask;

import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;

public class LocalSignalingProducer {

	public static final int TIMER_PERIOD = 60 * 1000;

	//// testing mode
	// public static final int TIMER_PERIOD = 10 * 1000;
	//// testing mode end

	public static final int SIGNALING_INTERVAL = 60 * 1000;

	public static String topic = "Signaling";
	public static String baseDir = "/data/DATA/SZ/";
	public static String startTime = "201501190000";
	public static String endTime = "201501190100";
	public static int timerInterval = TIMER_PERIOD;
	public static int signalingInterval = SIGNALING_INTERVAL;

	public static String host = "192.168.111.107:9092";

	/**
	 * Usage: java -jar SignalingProducer.jar Signaling /mnt2/sh/signaling/ 20160701000000 20160701235959 192.168.111.192:9092 6000 60000
	 */
	public static void main(String[] args) throws Exception {
		// TODO use commons-cli
		if (args.length > 0) {
			topic = args[0];
		}
		if (args.length > 1) {
			baseDir = args[1];
		}
		if (args.length > 2) {
			startTime = args[2];
		}
		if (args.length > 3) {
			endTime = args[3];
		}
		if (args.length > 4) {
			host = args[4];
		}
		if (args.length > 5) {
			timerInterval = Integer.parseInt(args[5]);
		}
		if (args.length > 6) {
			signalingInterval = Integer.parseInt(args[6]);
		}

		Properties props = new Properties();
		props.put("bootstrap.servers", host);
		props.put("acks", "all");
		props.put("retries", 0);
		props.put("batch.size", 16384);
		props.put("linger.ms", 1);
		props.put("buffer.memory", 33554432);
		props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
		props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");

		Producer<String, String> producer = new KafkaProducer<String, String>(props);
		Timer timer = new Timer();
		TimerTask task = new ReadFileTask(producer, topic, baseDir, startTime, endTime, signalingInterval);
		timer.schedule(task, 0, timerInterval);

		System.out.println("Press 'q' to exit.");
		Scanner s = new Scanner(System.in);
		while (s.hasNextLine()) {
			if ("q".equals(s.nextLine())) {
				producer.close();
				break;
			}
		}
		s.close();
	}
}

class ReadFileTask extends TimerTask {
	private Producer<String, String> producer;
	private String topic;
	private String baseDir;
	private long timeStamp = 0;
	private long endTime = 0;
	private int interval;
	private static SimpleDateFormat sdf = new SimpleDateFormat("yyyyMMddHHmmss");
	private static SimpleDateFormat logDateFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss.SSS");

	public ReadFileTask(Producer<String, String> producer, String topic, String baseDir, String startTime,
			String endTime, int interval) {
		this.producer = producer;
		this.topic = topic;
		this.baseDir = baseDir;
		try {
			this.timeStamp = sdf.parse(startTime).getTime();
		} catch (ParseException e) {
			this.timeStamp = 0;
		}
		try {
			this.endTime = sdf.parse(endTime).getTime();
		} catch (ParseException e) {
			this.endTime = 0;
		}
		this.interval = interval;
	}

	public void run() {
		if (timeStamp == 0) {
			timeStamp = System.currentTimeMillis();
		}
		if (timeStamp > this.endTime) {
			return;
		}

		StringBuilder filePath = new StringBuilder();
		String time = sdf.format(new Date(timeStamp));
		filePath.append(baseDir).append("TRAFF_").append(time).append(".txt");
		try {
			BufferedReader reader = new BufferedReader(
					new InputStreamReader(new FileInputStream(filePath.toString()), "GBK"));
			String strLine = reader.readLine();
			System.out.println("System time:\t" + logDateFormat.format(new Date(System.currentTimeMillis())));
			System.out.println("File time:\t" + logDateFormat.format(new Date(timeStamp)));
			System.out.println("FirstLine:\t" + strLine);
			int lineNo = 0;
			while (strLine != null) {
				lineNo++;
				// TODO check if the key should be unique.
				producer.send(new ProducerRecord<String, String>(topic, // "Signaling",
						timeStamp + "-" + Integer.toString(lineNo), strLine), new Callback() {
							public void onCompletion(RecordMetadata metadata, Exception e) {
								if (e != null) {
									e.printStackTrace();
								}
								// System.out.println("The offset of the record
								// we just sent is: " + metadata.offset());
							}
						});
				strLine = reader.readLine();
			}
			reader.close();
			System.out.println("Done time:\t" + logDateFormat.format(new Date(System.currentTimeMillis())));
			System.out.println("-----------------------");
		} catch (IOException ioe) {
			ioe.printStackTrace();
		}

		timeStamp += interval;
	}
}
