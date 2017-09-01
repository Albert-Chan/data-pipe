package com.dataminer.example.module;

import java.io.Serializable;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class StudentReaderSchema {

	private static final List<String> optionDef = Arrays.asList(
			"g,	group,	hasArg, required, , toString,	The application group",
			"i, input,	hasArg, required, , toString,	The HDFS input signaling path",
			"o, output,	hasArg, required, , toString,	The HDFS output path");

	private static Map<String, Class<?>> output = new HashMap<>();

	static {
		output.put("outputRDD", Student.class);
	};

	public static List<String> getOptionsDefinition() {
		return optionDef;
	}

	public static Class<?> getOutputSchema(String name) {
		return output.get(name);
	}

}

class Student implements Serializable {
	private static final long serialVersionUID = -3167129622840426500L;

	private String name;
	private int age;
	
	public Student(String name, int age) {
		this.name = name;
		this.age = age;
	}
	
	public String getName() {
		return name;
	}

	public int getAge() {
		return age;
	}

	public String toString() {
		return name + "," + age;
	}

}
