package com.dataminer.framework.pipeline;

import java.util.Arrays;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

import com.dataminer.monitor.AppEventTrigger;
import com.dataminer.monitor.MessageCombiner;
import com.dataminer.util.DailyResultSaver;
import com.dataminer.util.DateTimeWrapper;

/**
 * Facade class for passenger flow modules
 */
public class Pipeline {
	private JavaSparkContext ctx;

	private ConfigManager conf;
	protected static final AppEventTrigger TRIGGER = AppEventTrigger.get();
	
	
	public static void main(String[] args) throws Exception { 
		MessageCombiner mc = new MessageCombiner();
		if (!parseArgs(args)) {
			TRIGGER.send(mc.event("error", Arrays.deepToString(args)));
			return;
		}
		try {
			exec(mc);
		} catch (Exception e) {
			TRIGGER.send(mc.event("error", e.getMessage())); 
			throw e;
		}
	}
	
	private static void genMessageKey(MessageCombiner mc, JavaSparkContext ctx) {
		mc.partOfKey("appId", JavaSparkContext.toSparkContext(ctx).applicationId());
		mc.partOfKey("group", group);
		mc.partOfKey("analyticPeriod", analyticDay.formatTime("yyyy/MM/dd"));
	}
	

	public Pipeline(String appName, String[] args) {
		init(appName, args);
	}
	
	public boolean registerModule(String moduleMeta, Class<?> classType) {
		JavaRDD<T> A;
		return false;
	}

	private void start(String appName, String[] args) {
		eventTrigger = new AppEventTrigger(Metro.class.getName());
		conf = new ConfigManager();
		try {
			conf.parseArgs(args);
		} catch (Exception e) {
			eventTrigger.appStarted(Arrays.deepToString(args));
			eventTrigger.appStopped(Arrays.deepToString(args));
			throw new Exception("parse args error.");
		}
		
		DateTimeWrapper mt = conf.get("analyticDay");
		eventTrigger.appStarted("analyticPeriod=" + mt.formatTime("yyyy/MM/dd"));
		ctx = new JavaSparkContext(new SparkConf().setAppName(appName));
	}

	public Pipeline read(String inputFilePath) {
		currentRDD = ctx.textFile(inputFilePath + conf.get("analyticDay").formatTime("yyyy/MM/dd"));
		return this;
	}

	public Pipeline save() {
		// if (null != outputFilePath) {
		// msidRide.map(t -> t._1() + DELIMITER +
		// t._2().toExternalForm()).repartition(12)
		// .saveAsTextFile(outputFilePath +
		// analyticDay.formatTime("yyyy/MM/dd"));
		// }
		DailyResultSaver saver = new DailyResultSaver(ctx, conf.get("analyticDay").formatTime("yyyy/MM/dd"));
		saver.applyHDFSBasePath(conf.parser().getString("outputFilePath"));
		saver.save(currentRDD);
		return this;
	}



	public void end() {
		ctx.stop();
		eventTrigger.appStopped("analyticDay=" + analyticDay.formatTime("yyyy/MM/dd"));
		eventTrigger.close();
	}

}
