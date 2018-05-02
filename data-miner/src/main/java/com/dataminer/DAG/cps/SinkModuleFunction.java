package com.dataminer.DAG.cps;

import org.apache.spark.api.java.JavaRDD;

@FunctionalInterface
public interface SinkModuleFunction<T> {
	void apply(JavaRDD<T> rdd);
}
