package com.dataminer.util;

import java.util.Arrays;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.Row;

public class RDDUtil {

	public static JavaRDD<String> getGatheredRDD(JavaSparkContext ctx, List<String> paths) {
		JavaRDD<String> rdd = ctx.emptyRDD();
		for (String path : paths) {
			// TODO InvalidInputExcepton?
			rdd = rdd.union(ctx.textFile(path));
		}
		return rdd;
	}

	public static <K, V> JavaPairRDD<K, V> union(JavaSparkContext ctx, List<JavaPairRDD<K, V>> rdds) {
		JavaPairRDD<K, V> gatherRDD = JavaPairRDD.fromJavaRDD(ctx.emptyRDD());

		for (JavaPairRDD<K, V> rdd : rdds) {
			gatherRDD = gatherRDD.union(rdd);
		}

		return gatherRDD;
	}

	public synchronized static void deleteBeforeSaveToHDFS(JavaRDD<String> rdd, String hdfsPath) throws Exception {
		HDFSUtil.safeDelete(Arrays.asList(hdfsPath));
		rdd.saveAsTextFile(hdfsPath);
	}

	// [1,2,3] ===> 1,2,3
	public static JavaRDD<String> removeBracketOfRowRDD(JavaRDD<Row> rowRDD) {
		Pattern pattern = Pattern.compile("\\[(.*)\\]");

		JavaRDD<String> resultRDD = rowRDD.map(row -> {
			Matcher matcher = pattern.matcher(row.toString());
			if (matcher.find()) {
				return matcher.group(1);
			} else {
				throw new Exception("JavaRDD<Row> row format error");
			}
		});

		return resultRDD;
	}
}
