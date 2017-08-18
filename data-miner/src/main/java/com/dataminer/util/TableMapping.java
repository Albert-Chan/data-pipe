package com.dataminer.util;

import java.util.HashMap;
import java.util.stream.Collectors;

public class TableMapping {

	private static final String TABLE_MAPPING_PREFIX = "TAB_MAPPING.";
	private static final String TIME_INDEXER_PREFIX = "TIME_INDEXER.";
	
	private static final String DEFAULT_TIME_INDEXER_NAME = "TIME";

	public static String mapTableName(String embeddedTableName) {
		PassengerConfig psgConf = PassengerConfig.getConfig();
		return psgConf.getProperty(TABLE_MAPPING_PREFIX + embeddedTableName, embeddedTableName);
	}

	public static String[] getFieldsMapping(String embeddedTableName) {
		PassengerConfig psgConf = PassengerConfig.getConfig();
		String prefix = TABLE_MAPPING_PREFIX + embeddedTableName + ".";
		HashMap<String, String> filteredProps = psgConf.getPropertyWithPrefix(prefix);

		String[] fieldMapper = filteredProps.keySet().stream()
				.map(key -> key.replace(prefix, "") + " as " + filteredProps.get(key)).collect(Collectors.toList())
				.toArray(new String[0]);

		return fieldMapper;
	}
	
	public static String getTimeIndexer(String embeddedTableName) {
		PassengerConfig psgConf = PassengerConfig.getConfig();
		String indexer = TIME_INDEXER_PREFIX + embeddedTableName;
		return psgConf.getProperty(indexer, DEFAULT_TIME_INDEXER_NAME);
	}

}