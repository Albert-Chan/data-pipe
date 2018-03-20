package com.dataminer.example.module;

import java.util.List;
import java.util.Map;

import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

import com.dataminer.configuration.options.OptionDef;
import com.dataminer.configuration.options.ParsedOptions;
import com.dataminer.example.pojo.Student;
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

	public Collector(JavaSparkContext ctx, ParsedOptions options) {
		super(ctx, options);
	}
	
	public Collector(JavaSparkContext ctx, Map options) {
		super(ctx, options);
	}

	@Override
	public Schema getSchema() {
		return schema;
	}

	@Override
	public void exec() {
		@SuppressWarnings("unchecked")
		JavaRDD<Student> outputStudents = (JavaRDD<Student>) getInputValue(OUTPUT_STUDENT);
		showResult(outputStudents);
	}

	public static void showResult(JavaRDD<Student> outputStudents) {
		List<Student> filtered = outputStudents.collect();
		System.out.println(filtered.toString());
	}

}