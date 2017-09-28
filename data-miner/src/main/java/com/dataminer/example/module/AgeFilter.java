package com.dataminer.example.module;

import java.util.Arrays;
import java.util.List;

import org.apache.spark.api.java.JavaRDD;

import com.dataminer.configuration.options.ParsedOptions;
import com.dataminer.framework.pipeline.PipelineContext;
import com.dataminer.module.Module;
import com.dataminer.schema.Schema;
import com.dataminer.schema.Schema.BindingPort;

public class AgeFilter extends Module {
	
	public static final String ALL_STUDENT = "allStudent";
	public static final String FILTERED_STUDENT = "filteredStudent";
	
	private static Schema schema = new Schema();
	static {
		prepareSchema();
	}

	public static void prepareSchema() {
		List<String> optionDef = Arrays.asList("g,	group,	hasArg,	required, , toString, The application group name");
		schema.addOptionsDefinition(optionDef);
		schema.addInputSchema(new BindingPort(ALL_STUDENT, JavaRDD.class, "Student"));
		schema.addOutputSchema(new BindingPort(FILTERED_STUDENT, JavaRDD.class, "Student"));
	}

	public AgeFilter(String[] args, PipelineContext context) {
		super(args, context);
	}

	@Override
	public Schema getSchema() {
		return schema;
	}

	@Override
	public void exec(ParsedOptions parsedOptions) {
		@SuppressWarnings("unchecked")
		JavaRDD<Student> output = ((JavaRDD<Student>) getInputValue(ALL_STUDENT)).filter(s -> s.getAge() > 17);
		addOutputValue(FILTERED_STUDENT, output);
	}

}