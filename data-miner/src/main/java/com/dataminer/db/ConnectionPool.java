package com.dataminer.db;

import java.sql.Connection;
import java.sql.SQLException;

import org.apache.log4j.Logger;

import com.zaxxer.hikari.HikariDataSource;

public class ConnectionPool {
	private static final Logger LOG = Logger.getLogger(ConnectionPool.class);

	private HikariDataSource dataSource;

	ConnectionPool(HikariDataSource ds) {
		this.dataSource = ds;
	}

	public Connection getConnection() throws SQLException {
		LOG.info("Getting connection from " + dataSource.getPoolName());

		Connection conn = dataSource.getConnection();
		return conn;
	}

}
