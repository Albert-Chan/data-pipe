package com.dataminer.DAG.v1.module;

import org.apache.spark.api.java.JavaSparkContext;

import com.dataminer.configuration.options.ParsedOptions;

public abstract class SourceModule extends Module {

	public SourceModule(JavaSparkContext ctx, ParsedOptions options) {
		super(ctx, options);
	}

}
