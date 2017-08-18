package com.dataminer.util;

import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.UnsupportedEncodingException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.stream.Collectors;

import org.apache.commons.lang3.StringUtils;
import org.apache.log4j.Logger;

import com.dataminer.constants.Constants;

public class PassengerConfig implements Constants {
	private static final Logger LOG = Logger.getLogger(PassengerConfig.class);
	private Properties allProps = new Properties();

	private static final PassengerConfig instance = new PassengerConfig();

	public static PassengerConfig getConfig() {
		return instance;
	}

	private PassengerConfig() {
		try {
			loadConfig("/kafka.conf");
			loadConfig("/db.conf");
			loadConfig("/db_tbls_version.conf");
			loadConfig("/spark_serializer.conf");
			loadConfig("/tbl_mapping.conf");
		} catch (Exception e) {
			LOG.warn(e.getMessage());
		}
	}

	private void loadConfig(String filePath) throws IOException, UnsupportedEncodingException {
		Properties props = new Properties();
		try (InputStream input = this.getClass().getResourceAsStream(filePath)) {
			if (null != input) {
				props.load(new InputStreamReader(input, "UTF-8"));
				allProps.putAll(props);
			}
		}
	}

	public void addConfigFromJar(String fileName) throws IOException {
		loadConfig("/" + fileName);
	}

	public void addConfigFromFile(String filePath) throws IOException {
		// Does it work?
		loadConfig(filePath);
	}

	public Properties getBaseDBPoolProp() {
		Properties props = new Properties();
		props.setProperty("dataSourceClassName", getProperty("baseDataSourceClassName"));
		props.setProperty("dataSource.user", getProperty("baseDataSource.user"));
		props.setProperty("dataSource.password", getProperty("baseDataSource.password"));
		props.setProperty("dataSource.url", getProperty("baseDataSource.url"));
		return props;
	}

	public Properties getResultDBPoolProp() {
		Properties props = new Properties();
		props.setProperty("dataSourceClassName", getProperty("resultDataSourceClassName"));
		props.setProperty("dataSource.user", getProperty("resultDataSource.user"));
		props.setProperty("dataSource.password", getProperty("resultDataSource.password"));
		props.setProperty("dataSource.url", getProperty("resultDataSource.url"));
		return props;
	}

	public Properties getDBProp() {
		Properties props = new Properties();
		props.setProperty("driver", getProperty("db.driver"));
		props.setProperty("url", getProperty("db.connection.url"));
		props.setProperty("user", getProperty("db.username"));
		props.setProperty("password", getProperty("db.password"));
		return props;
	}

	public Map<String, String> getDBMap() {
		Map<String, String> dbMap = new HashMap<>();
		dbMap.put("driver", getProperty("db.driver"));
		dbMap.put("url", getProperty("db.connection.url"));
		dbMap.put("user", getProperty("db.username"));
		dbMap.put("password", getProperty("db.password"));
		return dbMap;
	}

	public Map<String, String> getDBInfo() {
		Map<String, String> dbInfo = new HashMap<String, String>();
		dbInfo.put("oracleDriver", getDBDriver());
		dbInfo.put("DBConnection", getDBConnection());
		dbInfo.put("user", getDBUser());
		dbInfo.put("password", getDBPassword());
		return dbInfo;
	}

	public String getDBDriver() {
		return getProperty(DB_DRIVER, ORACLE_DEFAULT_DRIVER);
	}

	public String getDBConnection() {
		return getProperty(DB_CONNECTION_URL);
	}

	public String getDBUser() {
		return getProperty(DB_USER);
	}

	public String getDBPassword() {
		return getProperty(DB_PASSWORD);
	}

	public String getDBTablesVersion(String tableName) {
		return getProperty(tableName);
	}

	public HashMap<String, Integer> getSignalingFieldIndexMap() {
		HashMap<String, Integer> fieldIndexMap = new HashMap<>();
		for (Map.Entry<Object, Object> item : allProps.entrySet()) {
			String field = (String) item.getKey();
			if (field.startsWith("meta") || field.startsWith("extra")) {
				Integer index = 0;
				try {
					index = Integer.parseInt((String) item.getValue());
				} catch (NumberFormatException e) {
					e.printStackTrace();
				}
				fieldIndexMap.put(field, index);
			}
		}
		return fieldIndexMap;
	}

	public String getKafkaBrokers() {
		return getProperty(KAFKA_BROKERS);
	}

	public String getMonitorTopicName() {
		return getProperty(KAFKA_MONITOR_TOPIC);
	}

	public String getProperty(String name) {
		return getProperty(name, StringUtils.EMPTY);
	}

	public HashMap<String, String> getPropertyWithPrefix(String prefix) {
		List<Object> matchedKeys = allProps.keySet().stream().filter(p -> p.toString().startsWith(prefix))
				.collect(Collectors.toList());
		HashMap<String, String> subMap = new HashMap<>();
		for (Object key : matchedKeys) {
			subMap.put((String) key, getProperty((String) key, StringUtils.EMPTY));
		}
		return subMap;
	}

	public String getProperty(String name, String defaultValue) {
		if (allProps.containsKey(name)) {
			return allProps.getProperty(name);
		} else {
			LOG.warn(name + " does not exist in the conf file!");
			return defaultValue;
		}
	}

	public int getProperty(String name, int defaultValue) {
		if (allProps.containsKey(name)) {
			try {
				return Integer.parseInt(allProps.getProperty(name));
			} catch (NumberFormatException e) {
				LOG.warn(e.getMessage());
				return defaultValue;
			}
		} else {
			LOG.warn(name + " does not exist in the conf file!");
			return defaultValue;
		}
	}

	public float getProperty(String name, float defaultValue) {
		if (allProps.containsKey(name)) {
			try {
				return Float.parseFloat(allProps.getProperty(name));
			} catch (NumberFormatException e) {
				LOG.warn(e.getMessage());
				return defaultValue;
			}
		} else {
			LOG.warn(name + " does not exist in the conf file!");
			return defaultValue;
		}
	}

}
