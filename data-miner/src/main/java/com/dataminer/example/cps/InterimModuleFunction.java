package com.dataminer.example.cps;

import org.apache.spark.api.java.JavaRDD;

@FunctionalInterface
public interface InterimModuleFunction<T> {
	void apply(JavaRDD<T> rdd);
}
