package com.dataminer.framework.pipeline;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;

public class PipelineContext {
	private String pipeName;
	private final JavaSparkContext sparkContext;

	public PipelineContext(String pipeName) {
		this.pipeName = pipeName;
		sparkContext = createJavaSparkContext();
	}

	private JavaSparkContext createJavaSparkContext() {
		SparkConf sparkConf = new SparkConf().setAppName(pipeName);
		JavaSparkContext ctx = new JavaSparkContext(sparkConf);
		return ctx;
	}

	public JavaSparkContext getJavaSparkContext() {
		return sparkContext;
	}

	public void close() {
		sparkContext.close();
	}

}
