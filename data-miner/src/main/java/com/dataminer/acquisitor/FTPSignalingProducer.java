package com.dataminer.acquisitor;

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Properties;
import java.util.Scanner;
import java.util.Timer;
import java.util.TimerTask;

import org.apache.commons.cli.BasicParser;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.Options;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class FTPSignalingProducer {
	private static Logger logger = LoggerFactory.getLogger(FTPSignalingProducer.class);

	private static final int SIGNALING_INTERVAL = 60 * 1000;

	private static final int TIMER_PERIOD = SIGNALING_INTERVAL;

	private static final String KAFKA_TOPIC = "Signaling";

	private static final int FTP_PORT = 21;

	private static String ftpServer;
	private static int ftpPort;
	private static String ftpUser;
	private static String ftpPassword;

	private static String topic;
	private static String remoteBase;

	private static int timerInterval;

	private static long startTimestamp = -1;
	private static long endTimestamp = Long.MAX_VALUE;

	private static String brokers;

	private static final ThreadLocal<SimpleDateFormat> sdf = ThreadLocal
			.withInitial(() -> new SimpleDateFormat("yyyyMMddHHmmss"));

	public static void main(String[] args) throws Exception {
		logger.info("Starting signaling transmission...");
		if (!parseArgs(args)) {
			System.exit(1);
		}

		Properties props = new Properties();
		props.put("bootstrap.servers", brokers);
		props.put("acks", "all");
		props.put("retries", 10);
		props.put("batch.size", 16384);
		props.put("linger.ms", 1);
		props.put("buffer.memory", 33554432);
		props.put("request.timeout.ms", 90000);
		props.put("max.block.ms", 120000);
		props.put("retry.backoff.ms", 1000);
		props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
		props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");

		// create kafka producer
		Producer<String, String> producer = new KafkaProducer<>(props);

		// create FTP downloader
		FTPDownloader downloader = new FTPDownloader(ftpServer, ftpPort, ftpUser, ftpPassword);
		if (startTimestamp != -1 || endTimestamp != Long.MAX_VALUE) {
			downloader.setFileChecker(new FileTimeChecker(startTimestamp, endTimestamp));
		}
		downloader.setPostHandler(new PostHandler(producer, topic));

		Timer timer = new Timer();
		TimerTask task = new DataAcquisitionTask(downloader, remoteBase);
		timer.schedule(task, 0, timerInterval);

		System.out.println("Press 'q' to exit.");
		Scanner s = new Scanner(System.in);
		while (s.hasNextLine()) {
			if ("q".equals(s.nextLine())) {
				timer.cancel();
				downloader.shutdown();
				producer.close();
				break;
			}
		}
		s.close();
	}

	private static boolean parseArgs(String[] args) {
		Options options = new Options();
		options.addOption("b", "brokers", true, "The kafka brokers.");
		options.addOption("t", "topic", true, "The kafka topic.");
		options.addOption("i", "timerInterval", true, "The interval for retrieving file.");

		options.addOption("r", "remoteBase", true, "The FTP server side base directory.");

		options.addOption("f", "ftpServer", true, "The ftp server.");
		options.addOption("fp", "ftpPort", true, "The ftp server port.");
		options.addOption("u", "ftpUser", true, "The ftp user name.");
		options.addOption("p", "ftpPassword", true, "The ftp password.");
		options.addOption("start", "startTime", true, "The signaling start time, in yyyyMMddHHmmss format.");
		options.addOption("end", "endTime", true, "The signaling end time, in yyyyMMddHHmmss format.");

		CommandLineParser parser = new BasicParser();
		HelpFormatter formatter = new HelpFormatter();
		try {
			CommandLine cmd = parser.parse(options, args);
			brokers = cmd.getOptionValue("brokers");
			topic = cmd.getOptionValue("topic", KAFKA_TOPIC);
			timerInterval = Integer.parseInt(cmd.getOptionValue("timerInterval", String.valueOf(TIMER_PERIOD)));

			remoteBase = cmd.getOptionValue("remoteBase");

			ftpServer = cmd.getOptionValue("ftpServer");
			ftpPort = Integer.parseInt(cmd.getOptionValue("ftpPort", String.valueOf(FTP_PORT)));
			ftpUser = cmd.getOptionValue("ftpUser");
			ftpPassword = cmd.getOptionValue("ftpPassword");

			String strStartTime = cmd.getOptionValue("startTime");
			String strEndTime = cmd.getOptionValue("endTime");

			if (strStartTime != null) {
				try {
					startTimestamp = sdf.get().parse(strStartTime).getTime();
				} catch (ParseException e) {
					startTimestamp = -1;
				}
			}
			if (strEndTime != null) {
				try {
					endTimestamp = sdf.get().parse(strEndTime).getTime();
				} catch (ParseException e) {
					endTimestamp = Long.MAX_VALUE;
				}
			}
		} catch (Exception e) {
			formatter.printHelp("", options);
			return false;
		}
		return true;
	}

}

class DataAcquisitionTask extends TimerTask {
	private FTPDownloader downloader;
	private String remoteBase;

	public DataAcquisitionTask(FTPDownloader downloader, String remoteBase) {
		this.downloader = downloader;
		this.remoteBase = remoteBase;
	}

	@Override
	public void run() {
		downloader.download(remoteBase);
	}

}
