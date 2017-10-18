package com.dataminer.datetime;

import java.io.Serializable;
import java.time.Instant;
import java.time.LocalDateTime;
import java.time.ZoneId;

public class TimeWindow implements Serializable {
	private static final long serialVersionUID = 8339895644550649179L;

	private LocalDateTime dateTime;

	private TimeWindow() {
	}

	public static TimeWindow of(String time, String pattern) {
		TimeWindow tw = new TimeWindow();
		tw.dateTime = LocalDateTime.parse(time, FastDateTimeFormatter.ofPattern(pattern));
		return tw;
	}

	public static TimeWindow ofLocalDateTime(LocalDateTime dateTime) {
		TimeWindow tw = new TimeWindow();
		tw.dateTime = dateTime;
		return tw;
	}

	public static TimeWindow ofTimeStamp(long timestamp) {
		TimeWindow tw = new TimeWindow();
		tw.dateTime = LocalDateTime.ofInstant(Instant.ofEpochMilli(timestamp), ZoneId.systemDefault());
		return tw;
	}

	public static TimeWindow from(TimeWindow t) {
		TimeWindow tw = new TimeWindow();
		tw.dateTime = t.dateTime;
		return tw;
	}

	/**
	 * Gets the time window index with the giving interval, indexed from 0.
	 * 
	 * @param intervalMinutes
	 *            the interval minutes, currently a time window is started from the
	 *            beginning of an hour, and an hour should be split into integral
	 *            time windows.
	 * @return the time window index
	 */
	public int getTimeWindowIndex(int intervalMinutes) {
		int hour = dateTime.getHour();
		int minute = dateTime.getMinute();
		int timeWindowIndex = (hour * 60 + minute) / intervalMinutes;
		return timeWindowIndex;
	}

	/**
	 * Rounds this local date time to the left edge of the time window.
	 * 
	 * @param intervalMinutes
	 *            the interval minutes, currently a time window is started from the
	 *            beginning of an hour, and an hour should be split into integral
	 *            time windows.
	 * @return the local date time representing the time window's left edge.
	 */
	public LocalDateTime toLeftEdge(int intervalMinutes) {
		return dateTime.withMinute(dateTime.getMinute() / intervalMinutes * intervalMinutes).withSecond(0);
	}

	/**
	 * Rounds this local date time to the right edge of the time window.
	 * 
	 * @param intervalMinutes
	 *            the interval minutes, currently a time window is started from the
	 *            beginning of an hour, and an hour should be split into integral
	 *            time windows.
	 * @return the local date time representing the time window's right edge.
	 */
	public LocalDateTime toRightEdge(int intervalMinutes) {
		return dateTime.withMinute(dateTime.getMinute() / intervalMinutes * intervalMinutes + intervalMinutes)
				.withSecond(0);
	}

}
