package com.dataminer.util;

import static org.apache.spark.sql.functions.round;

import java.sql.SQLException;
import java.time.LocalDate;
import java.time.format.DateTimeFormatter;
import java.util.Arrays;
import java.util.List;
import java.util.Properties;
import java.util.stream.Collectors;

import org.apache.log4j.Logger;
import org.apache.spark.sql.DataFrame;

import com.dataminer.configuration.ConfigManager;
import com.dataminer.constants.AnalyticTypes;
import com.dataminer.constants.DateTimeFormats;
import com.dataminer.db.ConnectionPool;
import com.dataminer.db.ConnectionPools;

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
	public static void dataFrame2DB(DataFrame df, String embeddedTableName, LocalDate date, AnalyticTypes type)
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
	public static void dataFrame2DBWithoutDeletion(DataFrame df, String embeddedTableName, LocalDate date,
			AnalyticTypes type) throws Exception {
		dataFrame2DB(df, embeddedTableName, date, type, false);
	}

	private static void dataFrame2DB(DataFrame df, String embeddedTableName, LocalDate date, AnalyticTypes type,
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
		DataFrameUtil.writeToTable(expandedDF, tableName, getSparkSQLProperties("result"));
	}

	/**
	 * Deletes data in the database with the same time period.
	 *
	 * @param outputTable
	 * @param analyticPeriod
	 * @param type
	 * @throws Exception
	 */
	private static void deleteDataInDBWithSamePeriod(String outputTable, LocalDate analyticPeriod,
			String timeIndexName, AnalyticTypes type) throws Exception {
		String tableFilter;
		switch (type) {
		case BY_MONTH:
			tableFilter = String.format(" from %s where %s = %d", outputTable, timeIndexName,
					analyticPeriod.getYear() * 100 + analyticPeriod.getMonth().getValue());
			checkAndDelete(tableFilter);
			break;
		case BY_DAY:
			String fromDate = analyticPeriod.format(DateTimeFormatter.ofPattern(DateTimeFormats.YMDHMS_FORMAT));
			String toDate = analyticPeriod.plusDays(1).format(DateTimeFormatter.ofPattern(DateTimeFormats.YMDHMS_FORMAT));
			tableFilter = String.format(" from %s where %s >= to_date('%s','%s') and %s < to_date('%s','%s')",
					outputTable, timeIndexName, fromDate, DateTimeFormats.ORACLE_DATA_FORMAT, timeIndexName, toDate,
					DateTimeFormats.ORACLE_DATA_FORMAT);
			checkAndDelete(tableFilter);
			break;
		default:
			throw new RuntimeException("Unknown analytic time type.");
		}
	}

	private static void checkAndDelete(String tableFilter) throws SQLException {
		ConnectionPool cp = ConnectionPools.get("result");
		String countSQL = "select count(*)" + tableFilter;
		long count = cp.sql(countSQL).executeQueryAndThen(rs -> {
			long recordCount = 0L;
			while (rs.next()) {
				recordCount = rs.getLong(1);
			}
			return recordCount;
		});
		
		if (count > 0L) {
			String deleteSQL = "delete" + tableFilter;
			cp.sql(deleteSQL).executeUpdate();
		}
	}

	private static DataFrame withColumnExpanded(DataFrame dataFrame, String outputTable, LocalDate date,
			AnalyticTypes type) throws Exception {
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
	
	private static Properties getSparkSQLProperties(String dbPoolName) {
		ConfigManager confManager = ConfigManager.getConfig();
		Properties props = new Properties();
		props.setProperty("driver", confManager.getProperty("db." + dbPoolName + "driver"));
		props.setProperty("url", confManager.getProperty("cp." + dbPoolName + "jdbcUrl"));
		props.setProperty("user", confManager.getProperty("cp" + dbPoolName + "username"));
		props.setProperty("password", confManager.getProperty("cp." + dbPoolName + "password"));
		return props;
	}

}
