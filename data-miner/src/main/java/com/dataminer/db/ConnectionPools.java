package com.dataminer.db;

import java.util.HashMap;
import java.util.Properties;

import org.apache.log4j.Logger;

import com.dataminer.configuration.ConfigManager;
import com.zaxxer.hikari.HikariConfig;
import com.zaxxer.hikari.HikariDataSource;

public class ConnectionPools {
	private static final Logger LOG = Logger.getLogger(ConnectionPools.class);

	private static HashMap<String, ConnectionPool> pools = new HashMap<>();

	public static synchronized ConnectionPool get(String poolName) {
		if (pools.get(poolName) == null) {
			Properties poolConfig = ConfigManager.getConfig().getPropertiesWithPrefix("cp." + poolName + ".");

			pools.put(poolName, new ConnectionPool(new HikariDataSource(new HikariConfig(poolConfig))));
			LOG.info("Pool " + poolName + " initialized.");
		}
		return pools.get(poolName);
	}
}
