package com.dataminer.example.module;

import java.util.List;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

import com.dataminer.example.cps.SinkModuleFunction;
import com.dataminer.monitor.AppEventTrigger;
import com.dataminer.monitor.MessageCombiner;

public class Example {

	private static final JavaSparkContext ctx = createContext();
	private static JavaSparkContext createContext() {
		SparkConf sparkConf = new SparkConf().setMaster("local").setAppName("example");
		JavaSparkContext ctx = new JavaSparkContext(sparkConf);
		return ctx;
	}
	
	public static JavaSparkContext getContext() {
		return ctx;
	}

	public static SinkModuleFunction<Student> sink = (JavaRDD<Student> s) -> {
		List<Student> filtered = s.collect();
		System.out.println(s);
		filtered.toString();
		System.out.println(filtered.toString());
	};

	private static final AppEventTrigger TRIGGER = AppEventTrigger.get();
	
	public static void main(String[] args) {
		
		/**
		 * args includes:
		 * appId, appName, overall options
		 */
		MessageCombiner mc = new MessageCombiner();
		mc.partOfKey("appId", JavaSparkContext.toSparkContext(getContext()).applicationId());
		mc.partOfKey("appName", "pipeline example");
		
		//TRIGGER.send(mc.event("action", "appStart"));
		System.out.println(mc.event("action", "appStart").getMessage());
		
		HDFSReader hdfsReader = new HDFSReader(new String[] {});
		StudentGenerator studentGen = new StudentGenerator(new String[] {});
		AgeFilter ageFilter = new AgeFilter(new String[] {});
		
		studentGen.bind("hdfsInput", hdfsReader, "hdfsOutput");
		ageFilter.bind("allStudent", studentGen, "allStudent");
		
		//TRIGGER.send(mc.event("action", "appEnd"));
		System.out.println(mc.event("action", "appEnd").getMessage());
	}

}
