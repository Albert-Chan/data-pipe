package com.dataminer.util;

import static org.apache.spark.sql.functions.round;

import java.sql.ResultSet;
import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;

import org.apache.log4j.Logger;
import org.apache.spark.sql.DataFrame;

import com.dataminer.constants.AnalyticTimeType;
import com.dataminer.constants.Constants;
import com.dataminer.db.HikariDBInstance;

import scala.Tuple3;

public class DataFrame2DBUtil {

	private static final Logger LOG = Logger.getLogger(DataFrame2DBUtil.class);

	/**
	 * Saves data into database. This method will <br>
	 * 1) mapping from embedded table/field name to the names in configuration;
	 * <br>
	 * 2) delete data in the database with the same time period;<br>
	 * 3) make sampling expansion;<br>
	 * 4) insert new data into database.
	 *
	 * @param df
	 *            the data frame
	 * @param embeddedTableName
	 *            the target embedded table name
	 * @param date
	 * @param type
	 * @throws Exception
	 */
	public static void dataFrame2DB(DataFrame df, String embeddedTableName, DateTimeWrapper date, AnalyticTimeType type)
			throws Exception {
		dataFrame2DB(df, embeddedTableName, date, type, true);
	}

	/**
	 * Saves data into database without deleting associate data. It is for real
	 * time purpose. This method will <br>
	 * 1) mapping from embedded table/field name to the names in configuration;
	 * <br>
	 * 2) make sampling expansion;<br>
	 * 3) insert new data into database.
	 *
	 * @param df
	 *            the data frame
	 * @param embeddedTableName
	 *            the target embedded table name
	 * @param date
	 * @param type
	 * @throws Exception
	 */
	public static void dataFrame2DBWithoutDeletion(DataFrame df, String embeddedTableName, DateTimeWrapper date,
			AnalyticTimeType type) throws Exception {
		dataFrame2DB(df, embeddedTableName, date, type, false);
	}

	private static void dataFrame2DB(DataFrame df, String embeddedTableName, DateTimeWrapper date, AnalyticTimeType type,
			boolean needDeletion) throws Exception {
		// mapping from embedded table/field name to the names in configuration.
		LOG.debug(" tableNameBeforeMapping " + embeddedTableName);
		String tableName = TableMapping.mapTableName(embeddedTableName);
		String[] fieldMapper = TableMapping.getFieldsMapping(embeddedTableName);
		LOG.debug(" tableNameBeforeMapping ==> tableName " + embeddedTableName + " ==> " + tableName + " fieldMapper "
				+ Arrays.stream(fieldMapper).collect(Collectors.toList()));
		String timeIndexerName = TableMapping.getTimeIndexer(embeddedTableName);

		if (needDeletion) {
			deleteDataInDBWithSamePeriod(tableName, date, timeIndexerName, type);
		}
		DataFrame expandedDF = withColumnExpanded(df.selectExpr(fieldMapper), tableName, date, type);

		// save to database
		DataFrameUtil.writeToTable(expandedDF, tableName, PassengerConfig.getConfig().getDBProp());
	}

	/**
	 * Deletes data in the database with the same time period.
	 *
	 * @param outputTable
	 * @param analyticPeriod
	 * @param type
	 * @throws Exception
	 */
	private static void deleteDataInDBWithSamePeriod(String outputTable, DateTimeWrapper analyticPeriod,
			String timeIndexName, AnalyticTimeType type) throws Exception {
		String tableFilter;
		switch (type) {
		case BY_MONTH:
			tableFilter = String.format(" from %s where %s = %d", outputTable, timeIndexName,
					analyticPeriod.getYearMonth());
			checkAndDelete(tableFilter);
			break;
		case BY_DAY:
			DateTimeWrapper date = new DateTimeWrapper(analyticPeriod.roundToDay());
			String fromDate = date.formatTime(Constants.YMDHMS_FORMAT);
			String toDate = date.addDays(1).formatTime(Constants.YMDHMS_FORMAT);
			tableFilter = String.format(" from %s where %s >= to_date('%s','%s') and %s < to_date('%s','%s')",
					outputTable, timeIndexName, fromDate, Constants.ORACLE_DATA_FORMAT, timeIndexName, toDate,
					Constants.ORACLE_DATA_FORMAT);
			checkAndDelete(tableFilter);
			break;
		default:
			throw new RuntimeException("Unknown analytic time type.");
		}
	}

	private static long getRecordsCount(HikariDBInstance hikariDBInstance, String countSQL) throws Exception {
		CheckedFunction<ResultSet, Long> rsToInteger = (ResultSet rs) -> {
			long count = 0L;
			while (rs.next()) {
				count = rs.getLong(1);
			}
			return count;
		};

		long count = hikariDBInstance.getSimpleQueryResult(countSQL, rsToInteger);
		return count;
	}

	private static void checkAndDelete(String tableFilter) throws Exception {
		HikariDBInstance hikariDBInstance = new HikariDBInstance(PassengerConfig.getConfig().getResultDBPoolProp());
		String countSQL = "select count(*)" + tableFilter;
		long count = getRecordsCount(hikariDBInstance, countSQL);
		if (count > 0L) {
			String deleteSQL = "delete" + tableFilter;
			hikariDBInstance.execute(deleteSQL);
		}
		hikariDBInstance.close();
	}

	private static DataFrame withColumnExpanded(DataFrame dataFrame, String outputTable, DateTimeWrapper date,
			AnalyticTimeType type) throws Exception {
		// get the sampling expansion <columnName, multiplier, condition>
		List<Tuple3<String, Float, String>> columnExpansion = SamplingExpansionUtil.getSamplingExpansion(outputTable,
				date, type);

		if (columnExpansion == null || columnExpansion.isEmpty()) {
			return dataFrame;
		}
		for (Tuple3<String, Float, String> pair : columnExpansion) {
			String columnName = pair._1();
			Float multiplier = pair._2();
			String condition = pair._3();

			if (condition != null && !condition.isEmpty()) {
				dataFrame = dataFrame.where(condition);
			}
			dataFrame = dataFrame.withColumn(columnName, round(dataFrame.col(columnName).multiply(multiplier)));
		}
		return dataFrame;
	}
	
	public static void dataFrame2DB(DataFrame df, String embeddedTableName, DateTimeWrapper date, AnalyticTimeType type,
			boolean needDeletion, String params) throws Exception {
		// mapping from embedded table/field name to the names in configuration.
		LOG.debug(" tableNameBeforeMapping " + embeddedTableName);
		String tableName = TableMapping.mapTableName(embeddedTableName);
		String[] fieldMapper = TableMapping.getFieldsMapping(embeddedTableName);
		LOG.debug(" tableNameBeforeMapping ==> tableName " + embeddedTableName + " ==> " + tableName + " fieldMapper "
				+ Arrays.stream(fieldMapper).collect(Collectors.toList()));
		String timeIndexerName = TableMapping.getTimeIndexer(embeddedTableName);

		if (needDeletion) {
			deleteDataInDBWithSamePeriod(tableName, date, timeIndexerName, type, params);
		}
		DataFrame expandedDF = withColumnExpanded(df.selectExpr(fieldMapper), tableName, date, type);

		// save to database
		DataFrameUtil.writeToTable(expandedDF, tableName, PassengerConfig.getConfig().getDBProp());
	}
	
	private static void deleteDataInDBWithSamePeriod(String outputTable, DateTimeWrapper analyticPeriod,
			String timeIndexName, AnalyticTimeType type, String params) throws Exception {
		String tableFilter;
		switch (type) {
		case BY_MONTH:
			tableFilter = String.format(" from %s where %s = %d", outputTable, timeIndexName,
					analyticPeriod.getYearMonth());
			checkAndDelete(tableFilter);
			break;
		case BY_DAY:
			DateTimeWrapper date = new DateTimeWrapper(analyticPeriod.roundToDay());
			String fromDate = date.formatTime(Constants.YMDHMS_FORMAT);
			String toDate = date.addDays(1).formatTime(Constants.YMDHMS_FORMAT);
			tableFilter = String.format(" from %s where %s >= to_date('%s','%s') and %s < to_date('%s','%s')",
					outputTable, timeIndexName, fromDate, Constants.ORACLE_DATA_FORMAT, timeIndexName, toDate,
					Constants.ORACLE_DATA_FORMAT);
			checkAndDelete(tableFilter);
			break;
		case BY_PARAM:
			tableFilter = String.format(" from %s where %s = %s", outputTable, timeIndexName,
					params);
			checkAndDelete(tableFilter);
			break;
		default:
			throw new RuntimeException("Unknown analytic time type.");
		}
	}

}