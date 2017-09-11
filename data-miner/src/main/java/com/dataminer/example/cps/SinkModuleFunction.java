package com.dataminer.example.cps;

import org.apache.spark.api.java.JavaRDD;

@FunctionalInterface
public interface SinkModuleFunction {
	void apply(JavaRDD rdd);
}
