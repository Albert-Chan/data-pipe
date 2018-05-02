package com.dataminer.db;

import java.sql.SQLException;
import java.util.List;

import org.junit.Test;

import com.clearspring.analytics.util.Lists;
import com.dataminer.DAG.v1.example.pojo.Student;

public class SQLExceutorTest {
	@Test
	public void test() {
		try {
			List<Student> output = ConnectionPools.get("develop").sql("select * from student").executeQueryAndThen(resultSet -> {
				List<Student> students = Lists.newArrayList();
				while (resultSet.next()) {
					String name = resultSet.getString("NAME");
					int age = resultSet.getInt("AGE");
					
					students.add(new Student(name, age));
				}
				return students;
			});
			
			System.out.println(output);
		} catch (SQLException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		
	}
}
