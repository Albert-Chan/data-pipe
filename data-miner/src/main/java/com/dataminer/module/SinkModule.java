package com.dataminer.module;

import java.util.Map;

import org.apache.spark.api.java.JavaSparkContext;

import com.dataminer.configuration.options.OptionsParser.OptionsParseException;
import com.dataminer.configuration.options.OptionsParser.OptionsParserBuildException;
import com.dataminer.configuration.options.ParsedOptions;

public abstract class SinkModule extends Module {

	public SinkModule(JavaSparkContext ctx, ParsedOptions options) {
		super(ctx, options);
	}
	
	public SinkModule(JavaSparkContext ctx, Map<String, Object> props) throws OptionsParserBuildException, OptionsParseException {
		super(ctx, props);
	}

}
