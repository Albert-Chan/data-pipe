package com.dataminer.db;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.ArrayList;

public class TransactionExecutor {

	private Connection conn;
	private ArrayList<SQLExecutor> executors = new ArrayList<>();

	private TransactionExecutor(Connection conn) {
		this.conn = conn;
	}

	public static TransactionExecutor through(Connection conn) throws SQLException {
		return new TransactionExecutor(conn);
	}

	public TransactionExecutor append(String sqlPattern) throws SQLException {
		executors.add(SQLExecutor.through(conn).sql(sqlPattern));
		return this;
	}

	public TransactionExecutor append(String sqlPattern, PSApplyParam<PreparedStatement> paramApplyFunc)
			throws SQLException {
		executors.add(SQLExecutor.through(conn).sql(sqlPattern).withParam(paramApplyFunc));
		return this;
	}

	public void executeTransaction() throws SQLException {
		boolean oldAutoCommit = conn.getAutoCommit();
		try {
			if (oldAutoCommit) {
				conn.setAutoCommit(false);
			}
			for (SQLExecutor executor : executors) {
				executor.executeUpdate();
			}
			conn.commit();
		} catch (SQLException e) {
			if (conn != null) {
				conn.rollback();
			}
			throw e;
		} finally {
			if (oldAutoCommit) {
				conn.setAutoCommit(oldAutoCommit);
			}
		}
	}

}
