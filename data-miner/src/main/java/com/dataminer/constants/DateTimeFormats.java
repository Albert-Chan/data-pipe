package com.dataminer.constants;

import java.time.format.DateTimeFormatter;

public interface DateTimeFormats {
	public static final String YM_FORMAT = "yyyy/MM";
	public static final String YMD_FORMAT = "yyyy/MM/dd";
	public static final String YMDHMS_FORMAT = "yyyy/MM/dd HH:mm:ss";
	public static final String H_M_FORMAT = "HH-mm";
	public static final String ORACLE_DATA_FORMAT = "yyyy/mm/dd hh24:mi:ss";

	public static final DateTimeFormatter FORMATTER_YM_FORMAT = DateTimeFormatter.ofPattern(DateTimeFormats.YM_FORMAT);
	public static final DateTimeFormatter FORMATTER_YMD_FORMAT = DateTimeFormatter
			.ofPattern(DateTimeFormats.YMD_FORMAT);
	public static final DateTimeFormatter FORMATTER_YMDHMS_FORMAT = DateTimeFormatter
			.ofPattern(DateTimeFormats.YMDHMS_FORMAT);
	public static final DateTimeFormatter FORMATTER_H_M_FORMAT = DateTimeFormatter
			.ofPattern(DateTimeFormats.H_M_FORMAT);
}