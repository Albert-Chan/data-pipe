package com.dataminer.util;

import java.util.HashMap;
import java.util.Map;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapred.JobConf;

public class FieldIndexMapping {
	public static Map<String, Integer> loadDefaultSignalingIdx(JobConf job) {
		Map<String, Integer> fieldsIndex = new HashMap<String, Integer>();

		if (job.get("msid") != null)
			fieldsIndex.put("msid", Integer.parseInt(job.get("msid")));
		else
			throw new RuntimeException("Could not get index of msid from configuration, "
					+ "please check if you attached signaling-field-index.xml");

		if (job.get("timestamp") != null)
			fieldsIndex.put("timestamp", Integer.parseInt(job.get("timestamp")));
		else
			throw new RuntimeException("Could not get index of timestamp from configuration, "
					+ "please check if you attached signaling-field-index.xml");

		if (job.get("lac") != null)
			fieldsIndex.put("lac", Integer.parseInt(job.get("lac")));
		else
			throw new RuntimeException("Could not get index of lac from configuration, "
					+ "please check if you attached signaling-field-index.xml");

		if (job.get("cellId") != null)
			fieldsIndex.put("cellId", Integer.parseInt(job.get("cellId")));
		else
			throw new RuntimeException("Could not get index of cellId from configuration, "
					+ "please check if you attached signaling-field-index.xml");

		if (job.get("eventId") != null)
			fieldsIndex.put("eventId", Integer.parseInt(job.get("eventId")));
		else
			throw new RuntimeException("Could not get index of eventId from configuration, "
					+ "please check if you attached signaling-field-index.xml");

		if (job.get("flag") != null)
			fieldsIndex.put("flag", Integer.parseInt(job.get("flag")));
		else
			throw new RuntimeException("Could not get index of flag from configuration, "
					+ "please check if you attached signaling-field-index.xml");

		return fieldsIndex;
	}

	public static Map<String, Integer> loadDefaultSignalingIdx(Configuration conf) {
		Map<String, Integer> fieldsIndex = new HashMap<String, Integer>();

		if (conf.get("msid") != null)
			fieldsIndex.put("msid", Integer.parseInt(conf.get("msid")));
		else
			throw new RuntimeException("Could not get index of msid from configuration, "
					+ "please check if you attached signaling-field-index.xml");

		if (conf.get("timestamp") != null)
			fieldsIndex.put("timestamp", Integer.parseInt(conf.get("timestamp")));
		else
			throw new RuntimeException("Could not get index of timestamp from configuration, "
					+ "please check if you attached signaling-field-index.xml");

		if (conf.get("lac") != null)
			fieldsIndex.put("lac", Integer.parseInt(conf.get("lac")));
		else
			throw new RuntimeException("Could not get index of lac from configuration, "
					+ "please check if you attached signaling-field-index.xml");

		if (conf.get("cellId") != null)
			fieldsIndex.put("cellId", Integer.parseInt(conf.get("cellId")));
		else
			throw new RuntimeException("Could not get index of cellId from configuration, "
					+ "please check if you attached signaling-field-index.xml");

		if (conf.get("eventId") != null)
			fieldsIndex.put("eventId", Integer.parseInt(conf.get("eventId")));
		else
			throw new RuntimeException("Could not get index of eventId from configuration, "
					+ "please check if you attached signaling-field-index.xml");

		if (conf.get("flag") != null)
			fieldsIndex.put("flag", Integer.parseInt(conf.get("flag")));
		else
			throw new RuntimeException("Could not get index of flag from configuration, "
					+ "please check if you attached signaling-field-index.xml");

		return fieldsIndex;
	}

	public static Map<String, Integer> loadExtraSignalingIdx(JobConf job) {
		Map<String, Integer> fieldsIndex = new HashMap<String, Integer>();

		if (job.get("cause") != null)
			fieldsIndex.put("cause", Integer.parseInt(job.get("cause")));

		if (job.get("secId") != null)
			fieldsIndex.put("secId", Integer.parseInt(job.get("secId")));

		if (job.get("res") != null)
			fieldsIndex.put("res", Integer.parseInt(job.get("res")));

		if (job.get("res1") != null)
			fieldsIndex.put("res1", Integer.parseInt(job.get("res1")));

		if (job.get("city") != null)
			fieldsIndex.put("city", Integer.parseInt(job.get("city")));

		return fieldsIndex;
	}

	public static Map<String, Integer> loadExtraSignalingIdx(Configuration conf) {
		Map<String, Integer> fieldsIndex = new HashMap<String, Integer>();

		if (conf.get("cause") != null)
			fieldsIndex.put("cause", Integer.parseInt(conf.get("cause")));

		if (conf.get("secId") != null)
			fieldsIndex.put("secId", Integer.parseInt(conf.get("secId")));

		if (conf.get("res") != null)
			fieldsIndex.put("res", Integer.parseInt(conf.get("res")));

		if (conf.get("res1") != null)
			fieldsIndex.put("res1", Integer.parseInt(conf.get("res1")));

		if (conf.get("city") != null)
			fieldsIndex.put("city", Integer.parseInt(conf.get("city")));

		return fieldsIndex;
	}
	
	public static Map<String, Integer> loadExtraSignalingIdx(Map<String, String> indexMap) {
		Map<String, Integer> fieldsIndex = new HashMap<String, Integer>();

		if (indexMap.get("cause") != null)
			fieldsIndex.put("cause", Integer.parseInt(indexMap.get("cause")));

		if (indexMap.get("secId") != null)
			fieldsIndex.put("secId", Integer.parseInt(indexMap.get("secId")));

		if (indexMap.get("res") != null)
			fieldsIndex.put("res", Integer.parseInt(indexMap.get("res")));

		if (indexMap.get("res1") != null)
			fieldsIndex.put("res1", Integer.parseInt(indexMap.get("res1")));

		if (indexMap.get("city") != null)
			fieldsIndex.put("city", Integer.parseInt(indexMap.get("city")));

		return fieldsIndex;
	}
}
