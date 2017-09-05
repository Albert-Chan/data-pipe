package com.dataminer.example.module;

import org.apache.spark.api.java.JavaRDD;

import com.dataminer.configuration.options.OptionsParser.OptionsParseException;
import com.dataminer.configuration.options.OptionsParser.OptionsParserBuildException;
import com.dataminer.configuration.options.ParsedOptions;
import com.dataminer.framework.pipeline.PipelineContext;
import com.dataminer.module.Module;

public class StudentReader extends Module {

	protected PipelineContext context;

	public boolean validate() {
		return true;
	}

	public void exec(ParsedOptions parsedOptions) throws OptionsParserBuildException, OptionsParseException {
		String input = parsedOptions.get("input");

		JavaRDD<Student> output = context.getSparkContext().textFile(input).map(line -> {
			String[] attrs = line.split(",");
			return new Student(attrs[0], Integer.parseInt(attrs[1]));
		});

		addOutputRDD("Student", Student.class, output);
	}

}