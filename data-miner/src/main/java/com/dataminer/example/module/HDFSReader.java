package com.dataminer.example.module;

import java.util.Arrays;

import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

import com.dataminer.configuration.options.OptionDef;
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
		OptionDef group = OptionDef.builder().longName("group").name("g").hasArg(true).required(true)
				.valueParser("toString").build();
		OptionDef input = OptionDef.builder().longName("input").name("i").hasArg(true).required(true)
				.valueParser("toString").description("The HDFS input path").build();
		schema.addOptionDefinitions(group, input);
		schema.addOutputSchema(new BindingPort(HDFS_OUTPUT, JavaRDD.class, "String"));
	}

	@Override
	public void exec(ParsedOptions parsedOptions) {
		String input = parsedOptions.get("input");
		// JavaRDD<String> output = context.textFile(input);

		JavaRDD<String> output = source(context.getJavaSparkContext());

		addOutputValue(HDFS_OUTPUT, output);
	}

	public static JavaRDD<String> source(JavaSparkContext context) {
		return context.parallelize(Arrays.asList("S1,17", "S2,18", "S3,23"));
	}

}