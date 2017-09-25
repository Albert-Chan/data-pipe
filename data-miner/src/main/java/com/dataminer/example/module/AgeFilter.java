package com.dataminer.example.module;

import java.util.Arrays;
import java.util.List;

import org.apache.spark.api.java.JavaRDD;

import com.dataminer.configuration.options.OptionsParser.OptionsParseException;
import com.dataminer.configuration.options.OptionsParser.OptionsParserBuildException;
import com.dataminer.configuration.options.ParsedOptions;
import com.dataminer.framework.pipeline.Context;
import com.dataminer.module.Module;
import com.dataminer.schema.Schema;
import com.dataminer.schema.Schema.BindingPort;

public class AgeFilter extends Module {
	public static Schema schema = new Schema();
	static {
		prepareSchema();
	}

	public static void prepareSchema() {
		List<String> optionDef = Arrays.asList("g,	group,	hasArg,	required, , toString, The application group name");
		schema.addOptionsDefinition(optionDef);
		schema.addInputSchema(new BindingPort("allStudent", "JavaRDD", "Student"));
		schema.addOutputSchema(new BindingPort("filteredStudent", "JavaRDD", "Student"));
	}

	public Schema getSchema() {
		return schema;
	}

	public AgeFilter(String[] args, Context context) {
		super(args, context);
	}

	public boolean validate() {
		return true;
	}

	public void exec(ParsedOptions parsedOptions) {
		JavaRDD<Student> output = ((JavaRDD<Student>) getInputValue("allStudent")).filter(s -> s.getAge() > 17);
		addOutputValue("filteredStudent", output);
	}

}