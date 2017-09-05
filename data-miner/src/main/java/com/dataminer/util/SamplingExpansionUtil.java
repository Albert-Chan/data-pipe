package com.dataminer.util;

import java.sql.SQLException;
import java.util.List;

import org.apache.log4j.Logger;

import com.clearspring.analytics.util.Lists;
import com.dataminer.configuration.PipelineConfig;
import com.dataminer.constants.AnalyticTimeType;
import com.dataminer.db.HikariDBInstance;

import scala.Tuple3;

public class SamplingExpansionUtil {
	private static final Logger LOG = Logger.getLogger(SamplingExpansionUtil.class);

	public static List<Tuple3<String, Float, String>> getColumnExpansion(HikariDBInstance hdb, String formattedDay,
	                                                                     String outputTable) throws SQLException {
		// get the sampling expansion
		String selectSQL = "select * from TBL_SAMPLING_EXPANSION where TABLE_NAME = ? and START_DATE <= DATE_FORMAT(?, 'yyyy/mm/dd') and (END_DATE > DATE_FORMAT(?, 'yyyy/mm/dd') or END_DATE is null)";

		List<Tuple3<String, Float, String>> cmc = hdb.getQueryResult(selectSQL, stmt -> {
			try {
				stmt.setString(1, outputTable);
				stmt.setString(2, formattedDay);
				stmt.setString(3, formattedDay);
			} catch (SQLException e) {
				LOG.error(e.getMessage());
			}
			return stmt;
		}, resultSet -> {
			List<Tuple3<String, Float, String>> columnExpansionPairs = Lists.newArrayList();

			try {
				while (resultSet.next()) {
					String columnName = resultSet.getString("COLUMN_NAME");
					float multiplier = resultSet.getFloat("MULTIPLIER");
					String condition = resultSet.getString("CONDITION");
					columnExpansionPairs.add(new Tuple3<>(columnName, multiplier, condition));
				}
			} catch (SQLException e) {
				LOG.error(e.getMessage());
			}

			return columnExpansionPairs;
		});
		return cmc;
	}

	public static List<Tuple3<String, Float, String>> getSamplingExpansion(String outputTable, DateTimeWrapper date,
	                                                                       AnalyticTimeType type) throws SQLException {
		switch (type) {
			case BY_MONTH:
				int day = date.getMaxDayOfMonth();
				date.setDay(day);
				break;
			case BY_DAY:
				break;
			default:
				return Lists.newArrayList();
		}

		int dayAsNumber = date.getDay();
		try (HikariDBInstance hdb = new HikariDBInstance(PipelineConfig.getDBProp("base"));) {
			// get the sampling expansion
			String selectSQL = "select * from TBL_SAMPLING_EXPANSION where TABLE_NAME = ? and START_DATE <= ? and (END_DATE > ? or END_DATE is null)";

			List<Tuple3<String, Float, String>> cmc = hdb.getQueryResult(selectSQL, stmt -> {
				try {
					stmt.setString(1, outputTable);
					stmt.setInt(2, dayAsNumber);
					stmt.setInt(3, dayAsNumber);
				} catch (SQLException e) {
					LOG.error(e.getMessage());
				}
				return stmt;
			}, resultSet -> {
				List<Tuple3<String, Float, String>> columnExpansionPairs = Lists.newArrayList();
				try {
					while (resultSet.next()) {
						String columnName = resultSet.getString("COLUMN_NAME");
						float multiplier = resultSet.getFloat("MULTIPLIER");
						String condition = resultSet.getString("CONDITION");
						columnExpansionPairs.add(new Tuple3<>(columnName, multiplier, condition));
					}
				} catch (SQLException e) {
					LOG.error(e.getMessage());
				}
				return columnExpansionPairs;
			});

			return cmc;
		}
	}

}
