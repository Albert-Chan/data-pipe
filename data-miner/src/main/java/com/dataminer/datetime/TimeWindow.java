package com.dataminer.datetime;

import static java.time.temporal.ChronoField.SECOND_OF_DAY;

import java.io.Serializable;
import java.time.Duration;
import java.time.Instant;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.time.temporal.TemporalAdjuster;

public class TimeWindow implements Serializable {
	private static final long serialVersionUID = 8339895644550649179L;

	private LocalDateTime startTime;
	private Duration interval;

	private TimeWindow(Duration interval) {
		this.interval = interval;
	}

	public static TimeWindow of(LocalDateTime startTime, Duration interval) {
		TimeWindow tw = new TimeWindow(interval);
		tw.startTime = startTime;
		return tw;
	}

	public static TimeWindow from(TimeWindow t) {
		return of(t.startTime, t.interval);
	}

	public static TimeWindow interval(Duration interval) {
		TimeWindow tw = new TimeWindow(interval);
		tw.startTime = LocalDateTime.of(1970, 1, 1, 0, 0, 0);
		return tw;
	}

	public TimeWindow withStart(String time, String pattern) {
		LocalDateTime startTime = LocalDateTime.parse(time, FastDateTimeFormatter.ofPattern(pattern));
		return of(startTime, interval);
	}

	public TimeWindow withStart(LocalDateTime startTime) {
		return of(startTime, interval);
	}

	public TimeWindow withStart(long timestamp) {
		LocalDateTime startTime = LocalDateTime.ofInstant(Instant.ofEpochMilli(timestamp), ZoneId.systemDefault());
		return of(startTime, interval);
	}

	/**
	 * Gets the time window index of the local date time from the the beginning,
	 * indexed from 0.
	 * 
	 * @param dateTime
	 *            the date time need to get time window index.
	 * @return the time window index
	 */
	public long getTimeWindowIndex(LocalDateTime dateTime) {
		long intervalSeconds = interval.getSeconds();
		long gap = Duration.between(startTime, dateTime).getSeconds();
		return gap / intervalSeconds;
	}

	/**
	 * Gets the time window index within a day, indexed from 0.
	 * 
	 * @param idateTime
	 *            the date time need to get time window index.
	 * @return the time window index
	 */
	public long getTimeWindowIndexInDayRange(LocalDateTime dateTime) {
		return getTimeWindowIndex(dateTime, RangeAdjusters.firstSecondOfDay());
	}

	private long getTimeWindowIndex(LocalDateTime dateTime, TemporalAdjuster adjuster) {
		TimeWindow newTimeWindow = this.withStart(dateTime.with(adjuster));
		return newTimeWindow.getTimeWindowIndex(dateTime);
	}

	/**
	 * Rounds a local date time to the left edge of the time window.
	 * 
	 * @param dateTime
	 *            the date time need to be aligned.
	 * @return the local date time representing the time window's left edge.
	 */
	public LocalDateTime toLeftEdge(LocalDateTime dateTime) {
		long index = getTimeWindowIndex(dateTime);
		return startTime.plusSeconds(index * interval.getSeconds());
	}

	/**
	 * Rounds a local date time to the right edge of the time window.
	 * 
	 * @param dateTime
	 *            the date time need to be aligned.
	 * @return the local date time representing the time window's right edge.
	 */
	public LocalDateTime toRightEdge(LocalDateTime dateTime) {
		long index = getTimeWindowIndex(dateTime);
		return startTime.plusSeconds((index + 1) * interval.getSeconds());
	}

	public static class RangeAdjusters {
		public static TemporalAdjuster firstSecondOfDay() {
			return temporal -> temporal.with(SECOND_OF_DAY, 0);
		}
	}

}
