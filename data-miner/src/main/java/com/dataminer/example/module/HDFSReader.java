package com.dataminer.example.module;

import java.util.Arrays;
import java.util.List;

import org.apache.spark.api.java.JavaRDD;

import com.dataminer.configuration.options.ParsedOptions;
import com.dataminer.framework.pipeline.Context;
import com.dataminer.module.Module;
import com.dataminer.schema.Schema;
import com.dataminer.schema.Schema.BindingPort;

public class HDFSReader extends Module {

	private static Schema schema = new Schema();
	static {
		prepareSchema();
	}

	public static void prepareSchema() {
		List<String> optionDef = Arrays.asList("g,	group,	hasArg, required, , toString,	The application group",
				"i, input,	hasArg, required, , toString,	The HDFS input path");

		schema.addOptionsDefinition(optionDef);
		schema.addOutputSchema(new BindingPort("hdfsOutput", "JavaRDD", "String"));
	}

	public Schema getSchema() {
		return schema;
	}

	public HDFSReader(String[] args, Context context) {
		super(args, context);
	}

	public boolean validate() {
		return super.validate();
	}

	@Override
	public void exec(ParsedOptions parsedOptions) {
		String input = parsedOptions.get("input");
		System.out.println(input);
		// JavaRDD<String> output = context.textFile(input);

		JavaRDD<String> output = context.getJavaSparkContext().parallelize(Arrays.asList("S1,17", "S2,18", "S3,23"));

		addOutputValue("hdfsOutput", output);
	}

}