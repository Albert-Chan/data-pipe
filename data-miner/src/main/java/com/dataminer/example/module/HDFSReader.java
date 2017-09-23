package com.dataminer.example.module;

import java.util.Arrays;
import java.util.List;

import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

import com.dataminer.configuration.options.OptionsParser.OptionsParseException;
import com.dataminer.configuration.options.OptionsParser.OptionsParserBuildException;
import com.dataminer.configuration.options.ParsedOptions;
import com.dataminer.module.Module;
import com.dataminer.schema.Schema;
import com.dataminer.schema.Schema.BindingPort;

public class HDFSReader extends Module {

	private JavaSparkContext context;
	public HDFSReader(String[] args, Context context) {
		super(args, context);
	}

	@Override
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
		System.out.println(input);
		//JavaRDD<String> output = context.textFile(input);
		
		JavaRDD<String> output = context.parallelize(Arrays.asList("S1,17", "S2,18", "S3,23"));
		
		addOutputValue("hdfsOutput", output);
	}

}