package com.dataminer.DAG.v1.example;

import java.util.Map;

import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

import com.dataminer.configuration.options.OptionDef;
import com.dataminer.configuration.options.ParsedOptions;
import com.dataminer.DAG.v1.Schema;
import com.dataminer.DAG.v1.Schema.BindingPort;
import com.dataminer.DAG.v1.module.Module;

public class HDFSReader extends Module {

	public static final String HDFS_OUTPUT = "hdfsOutput";
	public static Schema schema = new Schema();
	static {
		OptionDef group = OptionDef.builder().longName("group").name("g").hasArg(true).required(true)
				.valueParser("toString").build();
		OptionDef input = OptionDef.builder().longName("input").name("i").hasArg(true).required(true)
				.valueParser("toString").description("The HDFS input path").build();
		schema.addOptionDefinitions(group, input);
		schema.addOutputSchema(new BindingPort(HDFS_OUTPUT, JavaRDD.class, "String"));
	}

	@Override
	public Schema getSchema() {
		return schema;
	}

	public HDFSReader(JavaSparkContext ctx, ParsedOptions options) {
		super(ctx, options);
	}

	public HDFSReader(JavaSparkContext ctx, Map<String, Object> props) {
		super(ctx, props);
	}

	@Override
	public void exec() {
		String input = options.get("input");
		JavaRDD<String> output = context.getJavaSparkContext().textFile(input);
		addOutputValue(HDFS_OUTPUT, output);
	}

}