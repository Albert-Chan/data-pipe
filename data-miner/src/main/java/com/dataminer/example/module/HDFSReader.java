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

public class HDFSReader extends Module {

	public HDFSReader(String[] args) {
		super(args);
	}

	public void prepareSchema() {
		schema = new Schema();
		List<String> optionDef = Arrays.asList(
				"g,	group,	hasArg, required, , toString,	The application group",
				"i, input,	hasArg, required, , toString,	The HDFS input path");

		schema.addOptionsDefinition(optionDef);
		schema.addOutputSchema(new BindingPort("hdfsOutput", "JavaRDD", "String"));
	}

	public boolean validate() {
		return true;
	}

	@Override
	public void exec(ParsedOptions parsedOptions) throws OptionsParserBuildException, OptionsParseException {
		String input = parsedOptions.get("input");
		JavaRDD<String> output = ctx.getJavaSparkContext().textFile(input);
		addOutputValue("hdfsOutput", output);
	}

}