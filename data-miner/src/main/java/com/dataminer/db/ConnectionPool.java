package com.dataminer.db;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;

import org.apache.log4j.Logger;

import com.zaxxer.hikari.HikariDataSource;

public class ConnectionPool {
	private static final Logger LOG = Logger.getLogger(ConnectionPool.class);

	private HikariDataSource dataSource;

	ConnectionPool(HikariDataSource ds) {
		this.dataSource = ds;
	}

	private Connection getConnection() throws SQLException {
		Connection conn = dataSource.getConnection();
		LOG.info("Getting connection from " + dataSource.getPoolName());
		return conn;
	}

	public WrappedStatement prepareSQL(String sql) throws SQLException {
		try (Connection conn = getConnection(); PreparedStatement stmt = conn.prepareStatement(sql)) {
			LOG.info("Executing SQL: " + sql);
			return new WrappedStatement(stmt);
		} catch (SQLException e) {
			LOG.error("Error occoured while executing SQL: " + sql, e);
			throw e;
		}
	}

	public boolean tableExists(String tableName) {
		String tableExistsQuery = String.format("SELECT * FROM %s WHERE 1=0", tableName);
		try {
			prepareSQL(tableExistsQuery).executeQuery();
			return true;
		} catch (SQLException e) {
			return false;
		}
	}

	public void dropTable(String tableName) throws SQLException {
		String dropSQL = String.format("DROP TABLE %s", tableName);
		if (tableExists(tableName)) {
			try {
				prepareSQL(dropSQL).executeUpdate();
			} catch (SQLException e) {
				LOG.error("Error happend while dropping table " + tableName, e);
				throw e;
			}
		}
	}

	public void createTable(String tableName, String createSQL) throws InvalidTableNameException, SQLException {
		try {
			if (tableName == null || tableName.trim().equals("")) {
				throw new InvalidTableNameException("Table name '" + tableName + "' is invalid.");
			}

			if (!tableExists(tableName)) {
				try (Connection conn = getConnection(); PreparedStatement stmt = conn.prepareStatement(createSQL)) {
					stmt.execute();
					LOG.info(("create table " + tableName).toUpperCase());
				}
			}
		} catch (SQLException e) {
			LOG.error(e.getMessage(), e);
			throw e;
		}
	}

	public class InvalidTableNameException extends Exception {
		private static final long serialVersionUID = -5594173658313866696L;

		public InvalidTableNameException(String msg) {
			super(msg);
		}
	}

}
