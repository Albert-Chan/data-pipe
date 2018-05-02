package com.dataminer.job.submitter;

import org.apache.commons.cli.BasicParser;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.Options;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class JobDaemon {

	private static Logger logger = LoggerFactory.getLogger(JobDaemon.class);
	
	private static String topic;
	private static String group;
	private static String zkConnect;

	public static void main(String[] args) {
		if (!parseArgs(args)) {
			System.exit(1);
		}
		JobSubmitter submitter = new JobSubmitter(topic, group, zkConnect);
		logger.info("JobSubmitter created successfully.");
		submitter.handle();
	}

	private static boolean parseArgs(String[] args) {
		Options options = new Options();

		options.addOption("t", "topic", true, "The kafka topic.");
		options.addOption("g", "group", true, "The kafka topic group.");
		options.addOption("z", "zkConnect", true, "The zookeeper quorum.");

		CommandLineParser parser = new BasicParser();
		HelpFormatter formatter = new HelpFormatter();
		try {
			CommandLine cmd = parser.parse(options, args);
			topic = cmd.getOptionValue("topic");
			group = cmd.getOptionValue("group");
			zkConnect = cmd.getOptionValue("zkConnect");
		} catch (Exception e) {
			logger.error("Input options for JobDaemon are not correct. " + e.getMessage());
			formatter.printHelp("", options);
			return false;
		}
		return true;
	}
}
