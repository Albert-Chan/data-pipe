package com.dataminer.db;

import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;

import com.dataminer.util.SQLFunction;

public class WrappedStatement {

	private PreparedStatement ps;

	public WrappedStatement(PreparedStatement ps) {
		this.ps = ps;
	}

	public WrappedStatement withParam(SQLFunction<PreparedStatement, PreparedStatement> paramApplyFunc)
			throws SQLException {
		return new WrappedStatement(paramApplyFunc.apply(ps));
	}

	public ResultSet executeQuery() throws SQLException {
		return this.ps.executeQuery();
	}

	/**
	 * <code>
	 *
	 *</code>
	 * 
	 * 
	 * @param sql	the SQL string which may contain parameters
	 * @param mappedStatementFunc
	 *            changes made about the statement(closure), like
	 *            prepareStmt.setInt(1, 1528)
	 * @return affected rows
	 * @throws Exception
	 * 
	 */
	public <V> V executeQueryAndThen(SQLFunction<ResultSet, V> resultSetFunc) throws SQLException {
		return resultSetFunc.apply(executeQuery());
	}

	public int executeUpdate() throws SQLException {
		return this.ps.executeUpdate();
	}

}
