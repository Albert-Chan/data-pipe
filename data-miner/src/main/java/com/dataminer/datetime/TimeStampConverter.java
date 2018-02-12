package com.dataminer.datetime;

import java.time.Instant;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.time.ZonedDateTime;

public class TimeStampConverter {
	public static long localDateTime2TimeStamp(LocalDateTime ldt) {
		ZonedDateTime zdt = ldt.atZone(ZoneId.systemDefault());
		System.out.println(zdt);
		return zdt.toInstant().toEpochMilli();
	}

	public static long localDateTime2TimeStamp(LocalDateTime ldt, String zoneId) {
		ZonedDateTime zdt = ldt.atZone(ZoneId.of(zoneId));
		System.out.println(zdt);
		return zdt.toInstant().toEpochMilli();
	}

	public static LocalDateTime timeStamp2LocalDateTime(long timestamp) {
		return LocalDateTime.ofInstant(Instant.ofEpochMilli(timestamp), ZoneId.systemDefault());
	}

	public static LocalDateTime timeStamp2LocalDateTime(long timestamp, String zoneId) {
		return LocalDateTime.ofInstant(Instant.ofEpochMilli(timestamp), ZoneId.of(zoneId));
	}

}
