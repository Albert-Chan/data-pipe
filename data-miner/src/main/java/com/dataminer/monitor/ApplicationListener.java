package com.dataminer.monitor;

import org.apache.spark.JavaSparkListener;
import org.apache.spark.scheduler.SparkListenerApplicationEnd;
import org.apache.spark.scheduler.SparkListenerApplicationStart;

public class ApplicationListener extends JavaSparkListener  {

    @Override
    public void onApplicationStart(SparkListenerApplicationStart appStart) {
    	System.out.println("appStart   " + appStart.toString());
    }

    @Override
    public void onApplicationEnd(SparkListenerApplicationEnd appEnd) {
    	System.out.println("appEnd   " + appEnd.toString());
    }
}