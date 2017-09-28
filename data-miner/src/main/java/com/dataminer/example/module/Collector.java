package com.dataminer.example.module;

import java.util.Arrays;
import java.util.List;

import org.apache.spark.api.java.JavaRDD;

import com.dataminer.configuration.options.ParsedOptions;
import com.dataminer.framework.pipeline.PipelineContext;
import com.dataminer.module.SinkModule;
import com.dataminer.schema.Schema;
import com.dataminer.schema.Schema.BindingPort;

public class Collector extends SinkModule {
	private static Schema schema = new Schema();
	static {
		prepareSchema();
	}

	public static void prepareSchema() {
		List<String> optionDef = Arrays.asList("g,	group,	hasArg,	required, , toString, The application group name");
		schema.addOptionsDefinition(optionDef);
		schema.addInputSchema(new BindingPort("filteredStudent", "JavaRDD", "Student"));
	}
	
	public Collector(String[] args, PipelineContext context) {
		super(args, context);
	}

	@Override
	public Schema getSchema() {
		return schema;
	}

	@Override
	public boolean validate() {
		return true;
	}

	@Override
	public void exec(ParsedOptions parsedOptions) {
		@SuppressWarnings("unchecked")
		List<Student> filtered = ((JavaRDD<Student>) getInputValue("filteredStudent")).collect();
		System.out.println(filtered.toString());
	}

}