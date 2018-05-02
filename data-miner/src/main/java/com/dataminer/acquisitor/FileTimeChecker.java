package com.dataminer.acquisitor;

import java.text.ParseException;
import java.text.SimpleDateFormat;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class FileTimeChecker implements FileChecker {

	private static Logger logger = LoggerFactory.getLogger(FileTimeChecker.class);

	private static final ThreadLocal<SimpleDateFormat> sdf = ThreadLocal
			.withInitial(() -> new SimpleDateFormat("yyyyMMddHHmmss"));
	private long startTimestamp = -1;
	private long endTimestamp = Long.MAX_VALUE;

	public FileTimeChecker(long startTime, long endTime) {
		this.startTimestamp = startTime;
		this.endTimestamp = endTime;
	}

	public boolean check(String fileName) {
		String strTime = fileName.substring(fileName.indexOf("TRAFF_") + 6, fileName.indexOf(".txt"));
		try {
			long timestamp = sdf.get().parse(strTime).getTime();
			if (timestamp < this.startTimestamp || timestamp > this.endTimestamp) {
				return false;
			}
			return true;
		} catch (ParseException e) {
			logger.warn(e.getMessage());
			return false;
		}

	}

}
