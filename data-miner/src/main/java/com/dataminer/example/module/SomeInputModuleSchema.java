package com.dataminer.example.module;

import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class SomeInputModuleSchema {

	private static final List<String> optionDef = Arrays.asList(
			"g,	group,	hasArg, required, , toString,	The application group",
			"i, input,	hasArg, required, , toString,	The HDFS input signaling path",
			"o, output,	hasArg, required, , toString,	The HDFS output path");

	private static Map<String, Class<?>> output = new HashMap<>();

	static {
		output.put("outputRDD", String.class);
	};

	public static List<String> getOptionsDefinition() {
		return optionDef;
	}

	public static Class<?> getOutputSchema(String name) {
		return output.get(name);
	}

}
