package com.dataminer.configuration;

import java.util.Properties;

public class PipelineConfig {

	public static Properties getDBProp(String dbName) {
		ConfigManager confManager = ConfigManager.getConfig();
		Properties props = new Properties();
		props.setProperty("driver", confManager.getProperty("db." + dbName + "driver"));
		props.setProperty("url", confManager.getProperty("db." + dbName + "url"));
		props.setProperty("user", confManager.getProperty("db." + dbName + "username"));
		props.setProperty("password", confManager.getProperty("db." + dbName + "password"));
		return props;
	}

}
