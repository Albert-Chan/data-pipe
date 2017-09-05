package com.dataminer.util;

import java.io.IOException;

import org.apache.spark.SparkConf;

import com.dataminer.configuration.ConfigManager;

public class SerializerUtil {
	public static void checkKryoSerializer(SparkConf sparkConf, String pkgName) throws IOException {
		ConfigManager psgConf = ConfigManager.getConfig();
		if ("org.apache.spark.serializer.KryoSerializer".equals(psgConf.getProperty("spark.serializer")) ) {
			sparkConf.set("spark.serializer", "org.apache.spark.serializer.KryoSerializer");
			sparkConf.set("spark.kryo.registrator", pkgName + "." + "SparkKryoRegistrator");
			sparkConf.set("spark.kryo.registrationRequired", "true");
		}
	}

}
