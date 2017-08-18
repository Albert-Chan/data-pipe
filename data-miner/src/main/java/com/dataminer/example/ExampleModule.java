package com.dataminer.example;

import org.apache.spark.api.java.JavaPairRDD;

import com.dataminer.framework.pipeline.Module;

public class ExampleModule extends Module {

	JavaPairRDD<String, String> output;

	public void exec() {
		output = null;
	}

}
