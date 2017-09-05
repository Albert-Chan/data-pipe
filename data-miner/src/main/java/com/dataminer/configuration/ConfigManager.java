package com.dataminer.configuration;

import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.UnsupportedEncodingException;
import java.nio.charset.Charset;
import java.util.HashMap;
import java.util.List;
import java.util.Properties;
import java.util.stream.Collectors;

import org.apache.log4j.Logger;

public class ConfigManager {
	private static final Logger LOG = Logger.getLogger(ConfigManager.class);
	private static final String EMPTY_STRING = "";

	private Properties allProps = new Properties();

	private static final ConfigManager instance = new ConfigManager();

	public static ConfigManager getConfig() {
		return instance;
	}

	private ConfigManager() {
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
				props.load(new InputStreamReader(input, Charset.forName("UTF-8")));
				allProps.putAll(props);
			}
		}
	}

	public void addConfigFromJar(String fileName) throws IOException {
		loadConfig("/" + fileName);
	}

	public String getProperty(String name) {
		return getProperty(name, EMPTY_STRING);
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

	public HashMap<String, String> getPropertyWithPrefix(String prefix) {
		List<Object> matchedKeys = allProps.keySet().stream().filter(p -> p.toString().startsWith(prefix))
				.collect(Collectors.toList());
		HashMap<String, String> subMap = new HashMap<>();
		for (Object key : matchedKeys) {
			subMap.put((String) key, getProperty((String) key, EMPTY_STRING));
		}
		return subMap;
	}

}
