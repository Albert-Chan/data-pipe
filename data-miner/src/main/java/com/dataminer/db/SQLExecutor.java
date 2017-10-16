package com.dataminer.db;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;

public class SQLExecutor {

	private PreparedStatement ps;

	public SQLExecutor(Connection conn, String sqlPattern) throws SQLException {
		this.ps = conn.prepareStatement(sqlPattern);
	}

	public SQLExecutor withParam(SQLFunction<PreparedStatement, PreparedStatement> paramApplyFunc) throws SQLException {
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

}
