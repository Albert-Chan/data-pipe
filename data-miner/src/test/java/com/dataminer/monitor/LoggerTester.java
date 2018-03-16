package com.dataminer.monitor;

import org.apache.log4j.Logger;

public class LoggerTester {
	private static final Logger LOG = Logger.getLogger(LoggerTester.class);

	public static void main(String[] args) throws InterruptedException {
		LOG.info("logger tester started.");
		LOG.info(" ggg.");
		
		LOG.info("logger tester ended.");
		Thread.sleep(60000);
	}

}
