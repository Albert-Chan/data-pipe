package com.dataminer.example.module;

import org.apache.spark.api.java.JavaSparkContext;

import com.dataminer.framework.pipeline.Context;
import com.dataminer.monitor.AppEventTrigger;
import com.dataminer.monitor.MessageCombiner;

public class Example {

	private static final AppEventTrigger TRIGGER = AppEventTrigger.get();
	
	public static void main(String[] args) {
		Context context = new Context("example");
		/**
		 * args includes:
		 * appId, appName, overall options
		 */
		MessageCombiner mc = new MessageCombiner();
		mc.partOfKey("appId", JavaSparkContext.toSparkContext(context.getJavaSparkContext()).applicationId());
		mc.partOfKey("appName", "pipeline example");
		
		//TRIGGER.send(mc.event("action", "appStart"));
		System.out.println(mc.event("action", "appStart").getMessage());
		
		HDFSReader hdfsReader = new HDFSReader(new String[] {"-g", "test", "-i", "the input path"}, context);
		StudentGenerator studentGen = new StudentGenerator(new String[] {"-g", "test"}, context);
		AgeFilter ageFilter = new AgeFilter(new String[] {"-g", "test"}, context);
		Collector collector = new Collector(new String[] {"-g", "test"}, context);
		
		studentGen.bind("hdfsInput", hdfsReader, "hdfsOutput");
		ageFilter.bind("allStudent", studentGen, "allStudent");
		collector.bind("filteredStudent", ageFilter, "filteredStudent");
		
		collector.doTask();
		
		//TRIGGER.send(mc.event("action", "appEnd"));
		System.out.println(mc.event("action", "appEnd").getMessage());
		
		context.stop();
	}

}
