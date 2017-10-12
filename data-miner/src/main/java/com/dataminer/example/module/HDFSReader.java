package com.dataminer.example.module;

import java.util.Arrays;
import java.util.List;

import org.apache.spark.api.java.JavaRDD;

import com.dataminer.configuration.options.ParsedOptions;
import com.dataminer.framework.pipeline.PipelineContext;
import com.dataminer.module.Module;
import com.dataminer.schema.Schema;
import com.dataminer.schema.Schema.BindingPort;

public class HDFSReader extends Module {

	public static final String HDFS_OUTPUT = "hdfsOutput";
	public static Schema schema = new Schema();
	static {
		prepareSchema();
	}

	public HDFSReader(String[] args, PipelineContext context) {
		super(args, context);
	}
	
	@Override
	public Schema getSchema() {
		return schema;
	}

	public static void prepareSchema() {
		List<String> optionDef = Arrays.asList("g,	group,	hasArg, required, , toString,	The application group",
				"i, input,	hasArg, required, , toString,	The HDFS input path");

		schema.addOptionsDefinition(optionDef);
		schema.addOutputSchema(new BindingPort(HDFS_OUTPUT, JavaRDD.class, "String"));
	}

	@Override
	public void exec(ParsedOptions parsedOptions) {
		String input = parsedOptions.get("input");
		// JavaRDD<String> output = context.textFile(input);

		JavaRDD<String> output = context.getJavaSparkContext().parallelize(Arrays.asList("S1,17", "S2,18", "S3,23"));

		addOutputValue(HDFS_OUTPUT, output);
	}

}