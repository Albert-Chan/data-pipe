package com.dataminer.db;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;

public class SQLExecutor {

	private Connection conn;
	private PreparedStatement ps;
	private int batchSize = 1024;

	private SQLExecutor(Connection conn) {
		this.conn = conn;
	}

	public static SQLExecutor through(Connection conn) {
		return new SQLExecutor(conn);
	}

	public SQLExecutor batchSize(int newSize) {
		this.batchSize = newSize;
		return this;
	}

	// this is not thread safe
	public SQLExecutor sql(String sqlPattern) throws SQLException {
		this.ps = conn.prepareStatement(sqlPattern);
		return this;
	}

	public SQLExecutor withParam(PSApplyParam<PreparedStatement> paramApplyFunc) throws SQLException {
		paramApplyFunc.apply(ps);
		return this;
	}

	public ResultSet executeQuery() throws SQLException {
		try (PreparedStatement stmt = this.ps) {
			return stmt.executeQuery();
		}
	}

	public <V> V executeQueryAndThen(SQLFunction<ResultSet, V> resultSetFunc) throws SQLException {
		try (PreparedStatement stmt = this.ps) {
			return resultSetFunc.apply(stmt.executeQuery());
		}
	}

	public int executeUpdate() throws SQLException {
		try (PreparedStatement stmt = this.ps) {
			return stmt.executeUpdate();
		}
	}

	public <E> void executeBatch(Iterable<E> i, PSApplyExplicitParam<PreparedStatement, E> paramApplyFunc)
			throws SQLException {
		try (PreparedStatement stmt = this.ps;) {
			long count = 0L;
			for (E e : i) {
				paramApplyFunc.apply(ps, e);
				ps.addBatch();
				if (count % batchSize == 0) {
					ps.executeBatch();
				}
				count++;
			}
			ps.executeBatch();
		}
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

	public void dropTable(String tableName) throws InvalidTableNameException, SQLException {
		String dropSQL = String.format("DROP TABLE %s", tableName);
		if (tableExists(tableName)) {
			sql(dropSQL).executeUpdate();
		} else {
			throw new InvalidTableNameException("Table name '" + tableName + "' does NOT exist.");
		}

	}

	public void createTable(String tableName, String createSQL) throws InvalidTableNameException, SQLException {
		if (tableName == null || tableName.trim().equals("")) {
			throw new InvalidTableNameException("Table name '" + tableName + "' is invalid.");
		}
		if (tableExists(tableName)) {
			throw new InvalidTableNameException("Table name '" + tableName + "' already exists.");
		}
		sql(createSQL).executeUpdate();
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
