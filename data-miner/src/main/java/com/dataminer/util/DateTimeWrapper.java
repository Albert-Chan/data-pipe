package com.dataminer.util;

import org.joda.time.DateTime;

import java.io.Serializable;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.Date;
import java.util.GregorianCalendar;

public class DateTimeWrapper implements Comparable<DateTimeWrapper>, Serializable {
	private static final long serialVersionUID = 8339895644550649179L;
	
	private GregorianCalendar calendar;

	// TODO make it thread local to avoid repeated creation of SimpleDateFormat
	private SimpleDateFormat sdf = new SimpleDateFormat();

	public DateTimeWrapper(String time, String format) throws ParseException {
		sdf.applyPattern(format);
		Date date = sdf.parse(time);
		this.calendar = new GregorianCalendar();
		calendar.setTimeInMillis(date.getTime());
	}

	public DateTimeWrapper(long timestamp) {
		// TODO check the performance with DateFormat -> parseInt.
		this.calendar = new GregorianCalendar();
		calendar.setTimeInMillis(timestamp);
	}
	
	public DateTimeWrapper(DateTimeWrapper t) {
		this(t.getTimestamp());
	}

	public String formatTime(String pattern) {
		sdf.applyPattern(pattern);
		Date date = new Date(calendar.getTimeInMillis());
		return sdf.format(date);
	}

	public long getTimestamp() {
		return calendar.getTimeInMillis();
	}

	/**
	 * Gets the date(yyyyMMdd).
	 * 
	 * @return yyyyMMdd
	 */
	public int getDay() {
		int year = calendar.get(Calendar.YEAR);
		int month = calendar.get(Calendar.MONTH) + 1;
		int day = calendar.get(Calendar.DATE);
		int yyyyMMdd = year * 10000 + month * 100 + day;
		return yyyyMMdd;
	}
	
	/**
	 * Gets the yearMonth(yyyyMM).
	 * 
	 * @return yyyyMM
	 */
	public int getYearMonth() {
		int year = calendar.get(Calendar.YEAR);
		int month = calendar.get(Calendar.MONTH) + 1;
		int yyyyMM = year * 100 + month;
		return yyyyMM;
	}

	/**
	 * Gets the time index with the interval, indexed from 1.
	 * @return the time index
	 */
	public int getTimeIndex(Integer inteval) {
		int hour = calendar.get(Calendar.HOUR_OF_DAY);
		int minute = calendar.get(Calendar.MINUTE);
		int timeIndex = (hour * 60 + minute) / inteval + 1;
		return timeIndex;
	}

	/**
	 * Gets the time index with the interval 15 minutes, indexed from 1.
	 * @return the time index
	 */
	public int getTimeIndex() {
		return getTimeIndex(15);
	}

	/**
	 * Rounds this MeiHuiTime to the beginning of the day.
	 * @return the timestamp of the beginning of the day.
	 */
	public long roundToDay() {
		GregorianCalendar c = new GregorianCalendar();
		c.set(calendar.get(Calendar.YEAR), calendar.get(Calendar.MONTH), calendar.get(Calendar.DATE), 0, 0, 0);
		c.set(Calendar.MILLISECOND, 0);
		return c.getTimeInMillis();
	}

	/**
	 * Rounds this MeiHuiTime to the timestamp of the time index's left edge.
	 * @return  the timestamp of the time index's left edge.
	 */
	public long roundToTimeIndexLeftEdge() {
		int timeIndex = getTimeIndex();
		return roundToDay() + (timeIndex - 1) * 15 * 60 * 1000L;
	}
	
	/**
	 * Rounds this MeiHuiTime to the timestamp of the time index's right edge.
	 * @return  the timestamp of the time index's right edge.
	 */
	public long roundToTimeIndexRightEdge() {
		int timeIndex = getTimeIndex();
		return roundToDay() + timeIndex * 15 * 60 * 1000L;
	}

	/**
	 *  Gets the days from anotherDay to this.calendar
	 */
	public int getDaysDiff(DateTimeWrapper anotherDay) {
		int yearDiff = this.calendar.get(Calendar.YEAR) - anotherDay.calendar.get(Calendar.YEAR);
		if (yearDiff == 0) {
			return this.calendar.get(Calendar.DAY_OF_YEAR) - anotherDay.calendar.get(Calendar.DAY_OF_YEAR);
		} else if (yearDiff < 0) {
			return this.calendar.getActualMaximum(Calendar.DAY_OF_YEAR) - this.calendar.get(Calendar.DAY_OF_YEAR)
					+ anotherDay.calendar.get(Calendar.DAY_OF_YEAR)
					- anotherDay.calendar.getActualMinimum(Calendar.DAY_OF_YEAR);
		} else {
			return anotherDay.calendar.getActualMaximum(Calendar.DAY_OF_YEAR)
					- anotherDay.calendar.get(Calendar.DAY_OF_YEAR) + this.calendar.get(Calendar.DAY_OF_YEAR)
					- this.calendar.getActualMinimum(Calendar.DAY_OF_YEAR);
		}
	}

	/**
	 * Gets the max day of month
	 * @return
	 */
	public int getMaxDayOfMonth() {
		return this.calendar.getActualMaximum(Calendar.DAY_OF_MONTH);
	}

	/**
	 * Gets the seconds from 00:00:00 to the time in this.calendar.
	 * 
	 * @return the seconds lapsed in this day.
	 */
	public int getSecondsInDay() {
		int hour = calendar.get(Calendar.HOUR_OF_DAY);
		int minute = calendar.get(Calendar.MINUTE);
		int second = calendar.get(Calendar.SECOND);
		int secs = (hour * 60 + minute) * 60 + second;
		return secs;
	}

	/**
	 * Gets the DateTime(yyyyMMddHH) from a time stamp.
	 * 
	 * @return yyyyMMddHH
	 */
	public long getTimeToHour() {
		int hour = calendar.get(Calendar.HOUR_OF_DAY);
		return getDay() * 100L + hour;
	}

	/**
	 * Gets the DateTime(yyyyMMddHHmm) from a time stamp.
	 * 
	 * @return yyyyMMddHHmm
	 */
	public long getTimeToMinute() {
		int hour = calendar.get(Calendar.HOUR_OF_DAY);
		int minute = calendar.get(Calendar.MINUTE);
		return getDay() * 10000L + hour * 100 + minute;
	}

	/**
	 * Gets the day of the week.
	 * 
	 * @return 1 : SUNDAY; 2 : MONDAY; 3 : TUESDAY; 4 : WEDNESDAY; 5 : THURSDAY;
	 *         6 : FRIDAY; 7 : SATURDAY
	 */
	public int getDayOfWeek() {
		return calendar.get(Calendar.DAY_OF_WEEK);
	}
	
	public DateTimeWrapper addSeconds(int amount) {
		calendar.add(GregorianCalendar.SECOND, amount);
		return this;
	}

	public DateTimeWrapper addMinutes(int amount) {
		calendar.add(GregorianCalendar.MINUTE, amount);
		return this;
	}

	public DateTimeWrapper addHours(int amount) {
		calendar.add(GregorianCalendar.HOUR_OF_DAY, amount);
		return this;
	}

	public DateTimeWrapper addDays(int amount) {
		calendar.add(GregorianCalendar.DATE, amount);
		return this;
	}

	public void setDay(int day) {
		calendar.set(GregorianCalendar.DATE, day);
	}

	/**
	 * @return 1 : working day 0: weekend
	 */
	public boolean isWorkingDay() {
		if (getDayOfWeek() >= 2 && getDayOfWeek() <= 6) {
			return true;
		}
		return false;
	}

	public String[] computeDateInWeeks(int desiredWeeks, String datePattern) {
		if (desiredWeeks <= 0) {
			return new String[0];
		}
		long timestamp = calendar.getTimeInMillis();
		String[] dateArray = new String[desiredWeeks];
		SimpleDateFormat sdf = new SimpleDateFormat(datePattern);

		dateArray[desiredWeeks - 1] = sdf.format(timestamp);
		desiredWeeks--;
		while (desiredWeeks > 0) {
			timestamp -= 1000 * 60 * 60 * 24 * 7;
			DateTimeWrapper timeIndex = new DateTimeWrapper(timestamp);
			if ((isWorkingDay() && timeIndex.isWorkingDay()) || (!isWorkingDay() && !timeIndex.isWorkingDay())) {
				sdf.format(timestamp);
				dateArray[desiredWeeks - 1] = sdf.format(timestamp);
				desiredWeeks--;
			}
		}
		return dateArray;
	}

	/**
	 * Compute the desired number of dates which is before the timestamp,
	 * differentiate between working day and non-working day
	 * 
	 * @param timestamp
	 *            : till which date the computation ends
	 * @param desiredDays
	 *            : number of days to compute
	 * @param datePattern
	 *            : the date pattern for output
	 * @return an array of date which satisfy the datePattern
	 */
	public String[] computeDateInDays(int desiredDays, String datePattern) {
		if (desiredDays <= 0) {
			return new String[0];
		}
		long timestamp = calendar.getTimeInMillis();
		String[] dateArray = new String[desiredDays];
		SimpleDateFormat sdf = new SimpleDateFormat(datePattern);

		dateArray[desiredDays - 1] = sdf.format(timestamp);
		desiredDays--;
		while (desiredDays > 0) {
			timestamp -= 1000 * 60 * 60 * 24;
			DateTimeWrapper timeIndex = new DateTimeWrapper(timestamp);
			if ((isWorkingDay() && timeIndex.isWorkingDay()) || (!isWorkingDay() && !timeIndex.isWorkingDay())) {
				sdf.format(timestamp);
				dateArray[desiredDays - 1] = sdf.format(timestamp);
				desiredDays--;
			}
		}
		return dateArray;
	}
	
	public String[] computeDateInDays(int desiredDays, String datePattern, boolean ignoreWorkingDay) {
		if (desiredDays <= 0) {
			return new String[0];
		}
		long timestamp = calendar.getTimeInMillis();
		String[] dateArray = new String[desiredDays];
		SimpleDateFormat sdf = new SimpleDateFormat(datePattern);

		dateArray[desiredDays - 1] = sdf.format(timestamp);
		desiredDays--;
		while (desiredDays > 0) {
			timestamp -= 1000 * 60 * 60 * 24;
			DateTimeWrapper timeIndex = new DateTimeWrapper(timestamp);
			if (ignoreWorkingDay || (isWorkingDay() && timeIndex.isWorkingDay()) || (!isWorkingDay() && !timeIndex.isWorkingDay())) {
				sdf.format(timestamp);
				dateArray[desiredDays - 1] = sdf.format(timestamp);
				desiredDays--;
			}
		}
		return dateArray;
	}

	@Override
	public int compareTo(DateTimeWrapper o) {
		if (this.calendar.compareTo(o.calendar) < 0) {
			return -1;
		} else if (this.calendar.compareTo(o.calendar) > 0) {
			return 1;
		}
		return 0;
	}
	
	public long roundToTimeIndexLeftEdge( int interval )
	{
		int timeIndex = getTimeIndex(interval);
		return roundToDay( ) + ( timeIndex - 1 ) * interval * 60 * 1000L;
	}

	public long roundToTimeIndexRightEdge( int interval )
	{
		int timeIndex = getTimeIndex( );
		return roundToDay( ) + timeIndex * interval * 60 * 1000L;
	}

	public int getTimeIndex(int interval) {
		int hour = calendar.get(Calendar.HOUR_OF_DAY);
		int minute = calendar.get(Calendar.MINUTE);
		int timeIndex = (hour * 60 + minute) / interval + 1;
		return timeIndex;
	}


	public DateTime toDateTime() {
		return new DateTime(getTimestamp());
	}

	public static DateTimeWrapper fromDateTime(DateTime dateTime) {
		return new DateTimeWrapper(dateTime.getMillis());
	}
}
