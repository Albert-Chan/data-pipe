package com.dataminer.example.module;

import java.util.List;

import org.apache.spark.api.java.JavaRDD;

import com.dataminer.configuration.options.OptionDef;
import com.dataminer.configuration.options.ParsedOptions;
import com.dataminer.framework.pipeline.PipelineContext;
import com.dataminer.module.SinkModule;
import com.dataminer.schema.Schema;
import com.dataminer.schema.Schema.BindingPort;

public class Collector extends SinkModule {

	public static final String OUTPUT_STUDENT = "outputStudent";
	public static Schema schema = new Schema();
	static {
		prepareSchema();
	}

	public static void prepareSchema() {
		OptionDef group = OptionDef.builder().longName("group").name("g").hasArg(true).required(true)
				.valueParser("toString").build();
		schema.addOptionDefinitions(group);
		schema.addInputSchema(new BindingPort(OUTPUT_STUDENT, JavaRDD.class, "Student"));
	}

	public Collector(String[] args, PipelineContext context) {
		super(args, context);
	}

	@Override
	public Schema getSchema() {
		return schema;
	}

	@Override
	public void exec(ParsedOptions parsedOptions) {
		@SuppressWarnings("unchecked")
		JavaRDD<Student> outputStudents = (JavaRDD<Student>) getInputValue(OUTPUT_STUDENT);
		showResult(outputStudents);
	}

	public static void showResult(JavaRDD<Student> outputStudents) {
		List<Student> filtered = outputStudents.collect();
		System.out.println(filtered.toString());
	}

}