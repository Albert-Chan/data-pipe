package com.dataminer.util;

import java.util.Optional;

import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.DataFrame;
import org.apache.spark.sql.SQLContext;

import com.dataminer.constants.AnalyticTimeType;

public class ResultSaver {
	private JavaSparkContext ctx;
	private String analyticDay;
	private String params;

	private Optional<String> HDFSBasePath = Optional.empty();
	private Optional<String> tableName = Optional.empty();
	private Optional<Class<?>> schema = Optional.empty();

	public ResultSaver(JavaSparkContext ctx, String analyticDay, String params) {
		this.ctx = ctx;
		this.analyticDay = analyticDay;
		this.params = params;
	}

	public ResultSaver applyHDFSBasePath(String hdfsBasePath) {
		this.HDFSBasePath = Optional.ofNullable(hdfsBasePath);
		return this;
	}

	public <T> ResultSaver applyTable(String tableName, Class<T> schema) {
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
			DateTimeWrapper mhTime = new DateTimeWrapper(analyticDay, "yyyy/MM/dd");
			DataFrame2DBUtil.dataFrame2DB(df, tableName.get(), mhTime, AnalyticTimeType.BY_PARAM, true, params);
		}
	}
	
}
