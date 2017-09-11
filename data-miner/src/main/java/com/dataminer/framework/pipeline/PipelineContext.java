package com.dataminer.framework.pipeline;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;

import com.dataminer.configuration.ConfigManager;
import com.dataminer.monitor.AppEventTrigger;
import com.dataminer.monitor.MessageCombiner;

public class PipelineContext {
	private String pipeName;

	private JavaSparkContext ctx;
	private ConfigManager conf = ConfigManager.getConfig();
	//private OptionsParser optionsParser;
	private static final AppEventTrigger TRIGGER = AppEventTrigger.get();
	MessageCombiner mc = new MessageCombiner();

	// public void genMessageKey(MessageCombiner mc, JavaSparkContext ctx) {
	// mc.partOfKey("appId", JavaSparkContext.toSparkContext(ctx).applicationId());
	// mc.partOfKey("group", group);
	// mc.partOfKey("analyticPeriod", analyticDay.formatTime("yyyy/MM/dd"));
	// }
	public PipelineContext(String pipeName) {
		this.pipeName = pipeName;
	}
	
	public JavaSparkContext getSparkContext() {
		return ctx;
	}

	public void init(String[] args) {
		SparkConf sparkConf = new SparkConf().setAppName(pipeName);
		ctx = new JavaSparkContext(sparkConf);
		conf.addConfigFromJar(fileName);
	}

	protected void stop() {
		ctx.stop();
		TRIGGER.close();
	}
	

}
