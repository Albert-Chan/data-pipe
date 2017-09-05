package com.dataminer.example.module;

import org.apache.spark.api.java.JavaPairRDD;

import com.dataminer.configuration.options.ParsedOptions;
import com.dataminer.module.Module;

public class SomeInterimModule extends Module {

	JavaPairRDD<String, String> output;

	public void exec(ParsedOptions options) {
		output = null;
	}

}
