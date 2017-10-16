package com.dataminer.configuration;

import java.io.BufferedInputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.nio.charset.Charset;
import java.util.List;
import java.util.Properties;
import java.util.stream.Collectors;

import org.apache.log4j.Logger;

public class ConfigManager {
	private static final Logger LOG = Logger.getLogger(ConfigManager.class);
	private static final String EMPTY_STRING = "";

	private Properties allProps = new Properties();

	private static ConfigManager instance = null;

	static {
		try {
			instance = new ConfigManager();
		} catch (IOException e) {
			LOG.warn(e.getMessage());
		}
	}

	public static ConfigManager getConfig() {
		return instance;
	}

	private ConfigManager() throws IOException {
		String baseURL = System.getenv("DATAMINER_HOME");
		if (baseURL == null) {
			loadEmbededConfig("/kafka.conf");
			loadEmbededConfig("/db.conf");
		} else {
			for (File file : new File(baseURL).listFiles()) {
				loadExternalFile(file);
			}
		}
	}

	private void load(InputStream input) throws IOException {
		if (null != input) {
			Properties props = new Properties();
			props.load(new InputStreamReader(input, Charset.forName("UTF-8")));
			allProps.putAll(props);
		}
	}

	private void loadEmbededConfig(String filePath) throws IOException {
		try (InputStream input = this.getClass().getResourceAsStream(filePath)) {
			load(input);
		}
	}

	public void loadExternalFile(File file) throws IOException {
		try (InputStream input = new BufferedInputStream(new FileInputStream(file))) {
			load(input);
		}
	}

	public void loadExternalFile(String filePath) throws IOException {
		try (InputStream input = new BufferedInputStream(new FileInputStream(filePath))) {
			load(input);
		}
	}

	public String getProperty(String name) {
		return allProps.getProperty(name);
	}

	public String getProperty(String name, String defaultValue) {
		return allProps.getProperty(name, defaultValue);
	}

	public int getProperty(String name, int defaultValue) {
		String intAsString = getProperty(name);
		if (intAsString == null) {
			return defaultValue;
		} else {
			return Integer.parseInt(intAsString);
		}
	}

	public float getProperty(String name, float defaultValue) {
		String floatAsString = getProperty(name);
		if (floatAsString == null) {
			return defaultValue;
		} else {
			return Float.parseFloat(floatAsString);
		}
	}

	public Properties getPropertiesWithPrefix(String prefix) {
		List<Object> matchedKeys = allProps.keySet().stream().filter(p -> p.toString().startsWith(prefix))
				.collect(Collectors.toList());
		Properties subMap = new Properties();
		for (Object key : matchedKeys) {
			String strKey = (String) key;
			subMap.put(strKey.replaceFirst(prefix, ""), getProperty((String) key, EMPTY_STRING));
		}
		return subMap;
	}
}
