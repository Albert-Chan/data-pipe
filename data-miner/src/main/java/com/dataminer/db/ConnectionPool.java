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

	private Connection getConnection() throws SQLException {
		Connection conn = dataSource.getConnection();
		LOG.info("Getting connection from " + dataSource.getPoolName());
		return conn;
	}

	public SQLExecutor sql(String sql) throws SQLException {
		Connection conn = getConnection();
		return new SQLExecutor(conn, sql);
	}

	public boolean tableExists(String tableName) {
		String tableExistsQuery = String.format("SELECT * FROM %s WHERE 1=0", tableName);
		try {
			sql(tableExistsQuery).executeQuery();
			return true;
		} catch (SQLException e) {
			return false;
		}
	}

	public void dropTable(String tableName) throws SQLException {
		String dropSQL = String.format("DROP TABLE %s", tableName);
		if (tableExists(tableName)) {
			sql(dropSQL).executeUpdate();
		}
	}

	public void createTable(String tableName, String createSQL) throws InvalidTableNameException, SQLException {
		if (tableName == null || tableName.trim().equals("")) {
			throw new InvalidTableNameException("Table name '" + tableName + "' is invalid.");
		}
		if (!tableExists(tableName)) {
			sql(createSQL).executeUpdate();
		}
	}

	public class InvalidTableNameException extends Exception {
		private static final long serialVersionUID = -5594173658313866696L;

		public InvalidTableNameException(String msg) {
			super(msg);
		}
		
		public InvalidTableNameException(String message, Throwable cause) {
			super(message, cause);
		}
	}

}
