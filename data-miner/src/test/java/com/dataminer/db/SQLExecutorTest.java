package com.dataminer.db;

import java.sql.Connection;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;

import org.apache.log4j.Logger;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;
import static org.junit.Assert.assertEquals;

import com.google.common.collect.Lists;

import scala.Tuple2;

public class SQLExecutorTest {

	private static final Logger LOG = Logger.getLogger(SQLExecutorTest.class);
	private static Connection conn;

	@BeforeClass
	public static void getConnection() throws SQLException {
		conn = ConnectionPools.get("test").getConnection();
		LOG.info("Connection retrieved from the connection pool.");
	}

	@AfterClass
	public static void releaseConnection() throws SQLException {
		conn.close();
		LOG.info("Connection released to the connection pool.");
	}

	@Test
	public void test() throws SQLException, SQLExecutor.InvalidTableNameException {
		final String TBL_TEST = "TBL_TEST";

		SQLExecutor executor = SQLExecutor.through(conn);
		boolean tableExists = executor.tableExists(TBL_TEST);
		if (tableExists) {
			executor.dropTable(TBL_TEST);
		}
		// @formatter:off
		executor.createTable(TBL_TEST, 
				"CREATE TABLE `TBL_TEST` (\n" + 
				"  `timestamp` bigint(20) NOT NULL,\n" + 
				"  `vol` int(11) DEFAULT NULL,\n" + 
				"  PRIMARY KEY (`timestamp`)\n" + 
				") ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;");
		// @formatter:on
		testWrite();
		testRead();
		testWithParam();
	}

	public void testWrite() throws SQLException {
		SQLExecutor.through(conn).sql(String.format("insert into TBL_TEST values (%d, %d)", System.currentTimeMillis(),
				(int) (Math.random() * 1000))).executeUpdate();

	}

	public void testRead() throws SQLException {
		List<TestData> output = SQLExecutor.through(conn).sql("select * from TBL_TEST")
				.executeQueryAndThen(resultSet -> {
					List<TestData> testData = Lists.newArrayList();
					while (resultSet.next()) {
						long timestamp = resultSet.getLong("timestamp");
						int vol = resultSet.getInt("vol");

						testData.add(new TestData(timestamp, vol));
					}
					return testData;
				});
		LOG.info(output);
	}

	public void testWithParam() throws SQLException {
		long currentTime = System.currentTimeMillis();
		int randomInt = (int) (Math.random() * 1000);
		SQLExecutor.through(conn).sql("insert into TBL_TEST values (?, ?)").withParam(ps -> {
			ps.setLong(1, currentTime);
			ps.setInt(2, randomInt);
			return ps;
		}).executeUpdate();
	}

	@Test
	public void testBatch() throws SQLException {
		ArrayList<Tuple2<Long, Integer>> list = new ArrayList<>();
		for (int i = 0; i < 10_000; i++) {
			long currentTime = System.currentTimeMillis();
			int randomInt = (int) (Math.random() * 1000);
			Tuple2<Long, Integer> t = new Tuple2<>(currentTime, randomInt);
			list.add(t);
		}
		SQLExecutor.through(conn).sql("insert into TBL_TEST values (?, ?)").executeBatch(list, (ps, ele) -> {
			ps.setLong(1, ele._1);
			ps.setInt(2, ele._2);
			return ps;
		});

		LOG.info("Test batch done.");
	}

	@Test
	public void testTransaction() throws SQLException {
		boolean expectAutoCommit = conn.getAutoCommit();

		ArrayList<Tuple2<Long, Integer>> list = new ArrayList<>();
		for (int i = 1; i < 10_000; i++) {
			long currentTime = System.currentTimeMillis();
			int randomInt = (int) (Math.random() * 1000);
			Tuple2<Long, Integer> t = new Tuple2<>(currentTime, randomInt);
			list.add(t);
		}

		// @formatter:off
		TransactionExecutor.through(conn)
				.append("insert into TBL_TEST values (1, 2)")
				.append("insert into TBL_TEST values (2, 2)")
				.append("insert into TBL_TEST values (3, 2)")
				.executeTransaction();
		// @formatter:on

		// the auto commit flag should not change.
		assertEquals(expectAutoCommit, conn.getAutoCommit());
	}

}

class TestData {
	long timestamp;
	int vol;

	public TestData(long timestamp, int vol) {
		this.timestamp = timestamp;
		this.vol = vol;
	}
}