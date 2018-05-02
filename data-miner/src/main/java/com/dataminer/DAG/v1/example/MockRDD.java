package com.dataminer.DAG.v1.example;

import java.util.Arrays;
import java.util.Map;

import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

import com.dataminer.configuration.options.OptionDef;
import com.dataminer.configuration.options.ParsedOptions;
import com.dataminer.DAG.v1.Schema;
import com.dataminer.DAG.v1.Schema.BindingPort;
import com.dataminer.DAG.v1.module.Module;

public class MockRDD extends Module {

	public static final String OUTPUT = "mockOutput";
	public static Schema schema = new Schema();
	static {
		OptionDef group = OptionDef.builder().longName("group").name("g").hasArg(true).required(true)
				.valueParser("toString").build();
		schema.addOptionDefinitions(group);
		schema.addOutputSchema(new BindingPort(OUTPUT, JavaRDD.class, "String"));
	}

	@Override
	public Schema getSchema() {
		return schema;
	}

	public MockRDD(JavaSparkContext ctx, ParsedOptions options) {
		super(ctx, options);
	}

	public MockRDD(JavaSparkContext ctx, Map<String, Object> props) {
		super(ctx, props);
	}

	@Override
	public void exec() {
		JavaRDD<String> output = source(context.getJavaSparkContext());
		addOutputValue(OUTPUT, output);
	}

	public static JavaRDD<String> source(JavaSparkContext context) {
		return context.parallelize(Arrays.asList("S1,17", "S2,18", "S3,23"));
	}

}