package com.dataminer.framework.pipeline;

import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

import com.dataminer.example.SignalingModule;

public class HDFSCrawler extends Module {
	JavaSparkContext ctx;
	JavaRDD<String> output;

	public void validate() {

	}

	public void exec() {
		if (validate() && rerunIfExist) {
			String path = inputFilePath + analyticDay.formatTime("yyyy/MM/dd");
			output = ctx.textFile(path);
		}
	}
	
	
	
	
	
	
	
	
	
	
	public void getOutputRDD() {
		return output;
	}
	
	public void getOutputRDDByName() {
		
	}

}
