package com.dataminer.example.cps;

import org.apache.spark.api.java.JavaRDD;

@FunctionalInterface
public interface ModuleFunction<T> {
	void apply(JavaRDD<T> rdd);
}
