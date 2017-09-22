package com.dataminer.example.module;

import java.util.Arrays;
import java.util.List;

import org.apache.spark.api.java.JavaRDD;

import com.dataminer.configuration.options.OptionsParser.OptionsParseException;
import com.dataminer.configuration.options.OptionsParser.OptionsParserBuildException;
import com.dataminer.configuration.options.ParsedOptions;
import com.dataminer.module.Module;
import com.dataminer.schema.Schema;
import com.dataminer.schema.Schema.BindingPort;

public class AgeFilter extends Module {

	public AgeFilter(String[] args) {
		super(args);
	}

	public void prepareSchema() {
		schema = new Schema();
		List<String> optionDef = Arrays.asList("g,	group,	hasArg,	required, , toString, The application group name");
		schema.addOptionsDefinition(optionDef);
		schema.addInputSchema(new BindingPort("allStudent", "JavaRDD", "Student"));
		schema.addOutputSchema(new BindingPort("filteredStudent", "JavaRDD", "Student"));
	}

	public boolean validate() {
		return true;
	}

	public void exec(ParsedOptions parsedOptions) throws OptionsParserBuildException, OptionsParseException {
		JavaRDD<Student> output = ((JavaRDD<Student>) getInputValue("allStudent")).filter(s -> s.getAge() > 17);
		addOutputValue("filteredStudent", output);
	}

}