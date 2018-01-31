package com.dataminer.util;

import java.time.LocalDate;
import java.time.format.DateTimeFormatter;
import java.util.Optional;

import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.DataFrame;
import org.apache.spark.sql.SQLContext;

import com.dataminer.constants.AnalyticTimeType;

public class DailyResultSaver {
	private JavaSparkContext ctx;
	private String analyticDay;

	private Optional<String> HDFSBasePath = Optional.empty();
	private Optional<String> tableName = Optional.empty();
	private Optional<Class<?>> schema = Optional.empty();

	public DailyResultSaver(JavaSparkContext ctx, String analyticDay) {
		this.ctx = ctx;
		this.analyticDay = analyticDay;
	}

	public DailyResultSaver applyHDFSBasePath(String hdfsBasePath) {
		this.HDFSBasePath = Optional.ofNullable(hdfsBasePath);
		return this;
	}
	
	public <T> DailyResultSaver applyTable(String tableName) {
		this.tableName = Optional.ofNullable(tableName.toUpperCase());
		return this;
	}

	public <T> DailyResultSaver applyTable(String tableName, Class<T> schema) {
		this.tableName = Optional.ofNullable(tableName.toUpperCase());
		this.schema = Optional.ofNullable(schema);
		return this;
	}

	public <T> void save(JavaRDD<T> rdd) throws Exception {
		if (HDFSBasePath.isPresent()) {
			// save to HDFS and delete the same data at first
			String hdfsPath = HDFSBasePath.get() + analyticDay;
			HDFSUtil.exportToHDFS(hdfsPath, rdd);
		}

		if (tableName.isPresent() && schema.isPresent()) {
			// create data frame
			SQLContext sqlCtx = new SQLContext(ctx);
			DataFrame df = sqlCtx.createDataFrame(rdd, schema.get());

			// export to Database
			LocalDate date = LocalDate.parse(analyticDay, DateTimeFormatter.ofPattern("yyyy/MM/dd"));
			DataFrame2DBUtil.dataFrame2DB(df, tableName.get(), date, AnalyticTimeType.BY_DAY);
		}
	}
	
	public <T> void save(DataFrame df) throws Exception {
		if (HDFSBasePath.isPresent()) {
			// save to HDFS and delete the same data at first
			String hdfsPath = HDFSBasePath.get() + analyticDay;
			HDFSUtil.exportToHDFS(hdfsPath, df);
		}

		if (tableName.isPresent()) {
			// export to Database
			LocalDate date = LocalDate.parse(analyticDay, DateTimeFormatter.ofPattern("yyyy/MM/dd"));
			DataFrame2DBUtil.dataFrame2DB(df, tableName.get(), date, AnalyticTimeType.BY_DAY);
		}
	}
	
	public void clear() {
		HDFSBasePath = Optional.empty();
		tableName = Optional.empty();
		schema = Optional.empty();
	}
}
