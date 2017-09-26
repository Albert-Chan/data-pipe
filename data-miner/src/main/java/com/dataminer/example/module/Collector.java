package com.dataminer.example.module;

import java.util.Arrays;
import java.util.List;

import org.apache.spark.api.java.JavaRDD;

import com.dataminer.configuration.options.ParsedOptions;
import com.dataminer.framework.pipeline.Context;
import com.dataminer.module.Module;
import com.dataminer.schema.Schema;
import com.dataminer.schema.Schema.BindingPort;

public class Collector extends Module {
	private static Schema schema = new Schema();
	static {
		prepareSchema();
	}

	public static void prepareSchema() {
		List<String> optionDef = Arrays.asList("g,	group,	hasArg,	required, , toString, The application group name");
		schema.addOptionsDefinition(optionDef);
		schema.addInputSchema(new BindingPort("filteredStudent", "JavaRDD", "Student"));
	}

	public Schema getSchema() {
		return schema;
	}

	public Collector(String[] args, Context context) {
		super(args, context);
	}

	public boolean validate() {
		return true;
	}

	public void exec(ParsedOptions parsedOptions) {
		List<Student> filtered = ((JavaRDD<Student>) getInputValue("filteredStudent")).collect();
		System.out.println(filtered.toString());
	}

}