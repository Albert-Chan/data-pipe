package com.dataminer.example.module;

import java.util.Arrays;
import java.util.List;

import org.apache.spark.api.java.JavaRDD;

import com.dataminer.configuration.options.OptionsParser.OptionsParseException;
import com.dataminer.configuration.options.OptionsParser.OptionsParserBuildException;
import com.dataminer.example.cps.SinkModuleFunction;
import com.dataminer.configuration.options.ParsedOptions;
import com.dataminer.module.Context;
import com.dataminer.module.Module;
import com.dataminer.schema.Schema;
import com.dataminer.schema.Schema.BindingPort;

public class Collector extends Module {

	public Collector(String[] args, Context context) {
		super(args, context);
	}

	public void prepareSchema() {
		schema = new Schema();
		List<String> optionDef = Arrays.asList("g,	group,	hasArg,	required, , toString, The application group name");
		schema.addOptionsDefinition(optionDef);
		schema.addInputSchema(new BindingPort("filteredStudent", "JavaRDD", "Student"));
	}

	public boolean validate() {
		return true;
	}

	public void exec(ParsedOptions parsedOptions) throws OptionsParserBuildException, OptionsParseException {
		List<Student> filtered = ((JavaRDD<Student>) getInputValue("filteredStudent")).collect();
		System.out.println(filtered.toString());
	}

}