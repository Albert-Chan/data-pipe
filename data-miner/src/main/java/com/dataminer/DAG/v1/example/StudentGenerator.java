package com.dataminer.DAG.v1.example;

import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

import com.dataminer.configuration.options.OptionDef;
import com.dataminer.configuration.options.ParsedOptions;
import com.dataminer.DAG.v1.Schema;
import com.dataminer.DAG.v1.Schema.BindingPort;
import com.dataminer.DAG.v1.example.pojo.Student;
import com.dataminer.DAG.v1.module.Module;

public class StudentGenerator extends Module {

	public static final String HDFS_INPUT = "hdfsInput";
	public static final String ALL_STUDENT = "allStudent";
	private static Schema schema = new Schema();
	static {
		prepareSchema();
	}

	public StudentGenerator(JavaSparkContext ctx, ParsedOptions options) {
		super(ctx, options);
	}

	public static void prepareSchema() {
		OptionDef group = OptionDef.builder().longName("group").name("g").hasArg(true).required(true)
				.valueParser("toString").build();
		schema.addOptionDefinitions(group);
		schema.addInputSchema(new BindingPort(HDFS_INPUT, JavaRDD.class, "String"));
		schema.addOutputSchema(new BindingPort(ALL_STUDENT, JavaRDD.class, "Student"));
	}

	@Override
	public Schema getSchema() {
		return schema;
	}

	@Override
	public void exec() {
		@SuppressWarnings("unchecked")
		JavaRDD<Student> output = generateStudent((JavaRDD<String>) getInputValue(HDFS_INPUT));
		addOutputValue(ALL_STUDENT, output);
	}

	public static JavaRDD<Student> generateStudent(JavaRDD<String> hdfsInput) {
		return hdfsInput.map(line -> {
			String[] attrs = line.split(",");
			return new Student(attrs[0], Integer.parseInt(attrs[1]));
		});
	}

}