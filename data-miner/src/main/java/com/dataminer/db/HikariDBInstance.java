package com.dataminer.db;

import java.io.Serializable;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Map;
import java.util.Properties;
import java.util.function.Function;

import org.apache.log4j.Logger;

import com.dataminer.util.CheckedFunction;
import com.zaxxer.hikari.HikariConfig;
import com.zaxxer.hikari.HikariDataSource;

// only one per JVM
public class HikariDBInstance implements AutoCloseable, Serializable {
	private static final long serialVersionUID = -1296480901497097895L;
	private volatile Properties props;
	private static HikariDataSource dataSource;

	private static Logger LOG = Logger.getLogger(HikariDBInstance.class);

	public HikariDBInstance(Map<String, String> map) {
		Properties props = new Properties();
		for (Map.Entry<String, String> entry : map.entrySet()) {
			props.put(entry.getKey(), entry.getValue());
		}
		this.props = props;
	}

	public HikariDBInstance(Properties props) {
		this.props = props;
	}

	private synchronized HikariDataSource getDataSource() {
		if (dataSource == null) {
			LOG.info(" DataSource initialized ");
			dataSource = new HikariDataSource(new HikariConfig(props));
		}
		return dataSource;
	}

	public HikariDataSource renewDataSource(Properties props) {
		dataSource = new HikariDataSource(new HikariConfig(props));
		return dataSource;
	}

	public Connection getConnection() throws SQLException {
		Connection conn = getDataSource().getConnection();
		LOG.debug(" get connection " + conn);
		return conn;
	}

	public void execute(String sql) throws Exception {
		try (Connection conn = getConnection(); Statement stmt = conn.createStatement()) {
			LOG.info(" sql: " + sql);
			stmt.executeQuery(sql);
		} catch (Exception e) {
			LOG.error(e.getMessage() + " sql: " + sql, e);
			throw e;
		}
	}

	public <V> V getSimpleQueryResult(String sql, CheckedFunction<ResultSet, V> rsFunc) throws Exception {
		try (Connection conn = getConnection(); Statement stmt = conn.createStatement()) {
			LOG.info(" sql: " + sql);
			return rsFunc.apply(stmt.executeQuery(sql));
		} catch (Exception e) {
			LOG.error(e.getMessage() + " sql: " + sql, e);
			throw e;
		}
	}

	public <V> V getSimpleQueryResult(String sql, Function<ResultSet, V> rsFunc) throws SQLException {
		try (Connection conn = getConnection(); PreparedStatement stmt = conn.prepareStatement(sql)) {
			LOG.info(" sql: " + sql);
			return rsFunc.apply(stmt.executeQuery());
		} catch (SQLException e) {
			LOG.error(e.getMessage() + " sql: " + sql, e);
			throw e;
		}
	}

	/**
	 * @param sql
	 *            insert & update & delete sql eg: update TBL_JH_TAZ_S set POP_WORK
	 *            = -2000 where TAZID = ?
	 * @param mappedStatementFunc
	 *            changes made about the statement(closure), like
	 *            prepareStmt.setInt(1, 1528)
	 * @return affetced rows
	 * @throws Exception
	 *             simple example HikariDBInstance hdb = new HikariDBInstance();
	 *             String sql = "select * from TBL_JH_TAZ_S"; int TAZID = 1558;
	 *             String updateSQL = "update TBL_JH_TAZ_S set POP_WORK = -2000
	 *             where TAZID = ?"; Function<PreparedStatement, PreparedStatement>
	 *             mappedStatementFunc = (PreparedStatement stmt) -> { try {
	 *             stmt.setInt(1, TAZID); } catch (SQLException e) {
	 *             e.printStackTrace(); } return stmt; }; Integer result =
	 *             hdb.getUpdateResult(updateSQL, mappedStatementFunc);
	 */
	public Integer getUpdateResult(String sql,
			CheckedFunction<PreparedStatement, PreparedStatement> mappedStatementFunc) throws Exception {
		try (Connection conn = getConnection(); PreparedStatement stmt = conn.prepareStatement(sql)) {
			LOG.info(" sql: " + sql);
			return mappedStatementFunc.apply(stmt).executeUpdate();
		} catch (SQLException e) {
			LOG.error(e.getMessage() + " sql: " + sql, e);
			throw e;
		}
	}

	public <V> V getQueryResult(String sql, Function<PreparedStatement, PreparedStatement> mappedStatementFunc,
			Function<ResultSet, V> rsFunc) throws SQLException {
		try (Connection conn = getConnection(); PreparedStatement stmt = conn.prepareStatement(sql)) {
			LOG.info(" sql: " + sql);
			return rsFunc.apply(mappedStatementFunc.apply(stmt).executeQuery());
		} catch (SQLException e) {
			LOG.error(e.getMessage() + " sql: " + sql, e);
			throw e;
		}
	}

	public ResultSet getQueryResult(String sql, Function<PreparedStatement, PreparedStatement> mappedStatementFunc)
			throws SQLException {
		try {
			Connection conn = getConnection();
			PreparedStatement stmt = conn.prepareStatement(sql);
			LOG.info(" sql: " + sql);
			return mappedStatementFunc.apply(stmt).executeQuery();
		} catch (SQLException e) {
			LOG.error(e.getMessage() + " sql: " + sql, e);
			throw e;
		}
	}

	public ResultSet getQueryResult(String sql) throws SQLException {
		try {
			Connection conn = getConnection();
			PreparedStatement stmt = conn.prepareStatement(sql);
			LOG.info(" sql: " + sql);
			return stmt.executeQuery();
		} catch (SQLException e) {
			LOG.error(e.getMessage() + " sql: " + sql, e);
			throw e;
		}
	}

	public boolean existsTable(String tableName) {
		String tableExistsQuery = String.format("SELECT * FROM %s WHERE 1=0", tableName);
		LOG.debug(" tableExistsQuery " + tableExistsQuery);

		try (Connection conn = getConnection();
				PreparedStatement stmt = conn.prepareStatement(tableExistsQuery);
				ResultSet rs = stmt.executeQuery()) {
			return true;
		} catch (SQLException e) {
			LOG.warn(" " + tableName + " doesn't exist.", e);
			return false;
		}
	}

	// if the table does not exist, then create one; for truncate, please execute
	// command in Oracle
	public void createTable(String tableName, String createSQL) throws SQLException {
		try {
			if (tableName == null || tableName.trim().equals("")) {
				throw new RuntimeException("table name is invalid");
			}

			if (!existsTable(tableName)) {
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

	public void dropTable(String tableName) throws Exception {
		try {
			if (existsTable(tableName)) {
				String dropSQL = "DROP TABLE " + tableName;
				try (Connection conn = getConnection(); PreparedStatement stmt = conn.prepareStatement(dropSQL)) {
					stmt.execute();
					LOG.info(dropSQL);
				}
			}
		} catch (Exception e) {
			LOG.error(e.getMessage(), e);
			throw e;
		}
	}

	@Override
	public synchronized void close() {
		if (dataSource != null) {
			dataSource.close();
		}
		dataSource = null;
	}
}
