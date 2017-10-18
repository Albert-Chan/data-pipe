package com.dataminer.util;

import java.sql.SQLException;
import java.time.LocalDate;
import java.time.temporal.TemporalAdjusters;
import java.util.List;

import com.clearspring.analytics.util.Lists;
import com.dataminer.constants.AnalyticTimeType;
import com.dataminer.db.ConnectionPools;

import scala.Tuple3;

public class SamplingExpansionUtil {

	public static List<Tuple3<String, Float, String>> getSamplingExpansion(String outputTable, LocalDate date,
			AnalyticTimeType type) throws SQLException {
		switch (type) {
		case BY_MONTH:
			date = date.with(TemporalAdjusters.lastDayOfMonth());
			break;
		case BY_DAY:
			break;
		default:
			return Lists.newArrayList();
		}

		int dayAsNumber = date.getDayOfMonth();

		// get the sampling expansion
		String selectSQL = "select * from TBL_SAMPLING_EXPANSION where TABLE_NAME = ? and START_DATE <= ? and (END_DATE > ? or END_DATE is null)";

		List<Tuple3<String, Float, String>> cmc = ConnectionPools.get("base").sql(selectSQL).withParam(stmt -> {
			stmt.setString(1, outputTable);
			stmt.setInt(2, dayAsNumber);
			stmt.setInt(3, dayAsNumber);
			return stmt;
		}).executeQueryAndThen(resultSet -> {
			List<Tuple3<String, Float, String>> columnExpansionPairs = Lists.newArrayList();
			while (resultSet.next()) {
				String columnName = resultSet.getString("COLUMN_NAME");
				float multiplier = resultSet.getFloat("MULTIPLIER");
				String condition = resultSet.getString("CONDITION");
				columnExpansionPairs.add(new Tuple3<>(columnName, multiplier, condition));
			}
			return columnExpansionPairs;
		});

		return cmc;
	}

}
