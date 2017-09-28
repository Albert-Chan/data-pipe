package com.dataminer.example.module;

import java.util.Arrays;
import java.util.List;

import org.apache.spark.api.java.JavaRDD;

import com.dataminer.configuration.options.ParsedOptions;
import com.dataminer.framework.pipeline.PipelineContext;
import com.dataminer.module.Module;
import com.dataminer.schema.Schema;
import com.dataminer.schema.Schema.BindingPort;

public class StudentGenerator extends Module {

	public static final String HDFS_INPUT = "hdfsInput";
	public static final String ALL_STUDENT = "allStudent";
	private static Schema schema = new Schema();
	static {
		prepareSchema();
	}

	public StudentGenerator(String[] args, PipelineContext context) {
		super(args, context);
	}

	public static void prepareSchema() {
		List<String> optionDef = Arrays.asList("g,	group,	hasArg, required, , toString,	The application group");
		schema.addOptionsDefinition(optionDef);
		schema.addInputSchema(new BindingPort(HDFS_INPUT, JavaRDD.class, "String"));
		schema.addOutputSchema(new BindingPort(ALL_STUDENT, JavaRDD.class, "Student"));
	}

	@Override
	public Schema getSchema() {
		return schema;
	}

	@Override
	public void exec(ParsedOptions parsedOptions) {
		@SuppressWarnings("unchecked")
		JavaRDD<Student> output = ((JavaRDD<String>) getInputValue(HDFS_INPUT)).map(line -> {
			String[] attrs = line.split(",");
			return new Student(attrs[0], Integer.parseInt(attrs[1]));
		});
		addOutputValue(ALL_STUDENT, output);
	}

}