package com.dataminer.framework.pipeline;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;

import com.dataminer.configuration.ConfigManager;
import com.dataminer.monitor.AppEventTrigger;
import com.dataminer.monitor.MessageCombiner;

public class Context {
	private String pipeName;

	private ConfigManager conf = ConfigManager.getConfig();
	//private OptionsParser optionsParser;
	private static final AppEventTrigger TRIGGER = AppEventTrigger.get();
	MessageCombiner mc = new MessageCombiner();

//	public void genMessageKey(MessageCombiner mc, JavaSparkContext ctx) {
//		mc.partOfKey("appId", JavaSparkContext.toSparkContext(ctx).applicationId());
//		mc.partOfKey("group", group);
//		mc.partOfKey("analyticPeriod", analyticDay.formatTime("yyyy/MM/dd"));
//	}
	
	private static final JavaSparkContext ctx = createContext();
	private static JavaSparkContext createContext() {
		SparkConf sparkConf = new SparkConf().setMaster("local").setAppName("example");
		JavaSparkContext ctx = new JavaSparkContext(sparkConf);
		return ctx;
	}
	
	public static JavaSparkContext getContext() {
		return ctx;
	}	

	public Context(String pipeName) {
		this.pipeName = pipeName;
	}
	
	public JavaSparkContext getJavaSparkContext() {
		return ctx;
	}

//	public void init(String[] args) {
//		SparkConf sparkConf = new SparkConf().setAppName(pipeName);
//		ctx = new JavaSparkContext(sparkConf);
//		conf.addConfigFromJar("configFileName");
//	}

	public void stop() {
		ctx.stop();
		TRIGGER.close();
	}
	

}
