package com.dataminer.example.module;

import java.util.Arrays;
import java.util.List;

import org.apache.spark.api.java.JavaRDD;

import com.dataminer.configuration.options.ParsedOptions;
import com.dataminer.framework.pipeline.Context;
import com.dataminer.module.Module;
import com.dataminer.schema.Schema;
import com.dataminer.schema.Schema.BindingPort;

public class StudentGenerator extends Module {

	public static Schema schema = new Schema();
	static {
		prepareSchema();
	}

	public static void prepareSchema() {
		List<String> optionDef = Arrays.asList("g,	group,	hasArg, required, , toString,	The application group");
		schema.addOptionsDefinition(optionDef);
		schema.addInputSchema(new BindingPort("hdfsInput", "JavaRDD", "String"));
		schema.addOutputSchema(new BindingPort("allStudent", "JavaRDD", "Student"));
	}

	public Schema getSchema() {
		return schema;
	}

	public StudentGenerator(String[] args, Context context) {
		super(args, context);
	}

	public boolean validate() {
		return true;
	}

	public void exec(ParsedOptions parsedOptions) {
		JavaRDD<Student> output = ((JavaRDD<String>) getInputValue("hdfsInput")).map(line -> {
			String[] attrs = line.split(",");
			return new Student(attrs[0], Integer.parseInt(attrs[1]));
		});
		addOutputValue("allStudent", output);
	}

}