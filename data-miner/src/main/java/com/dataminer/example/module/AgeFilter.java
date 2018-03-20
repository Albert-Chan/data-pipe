package com.dataminer.example.module;

import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

import com.dataminer.configuration.options.OptionDef;
import com.dataminer.configuration.options.ParsedOptions;
import com.dataminer.example.pojo.Student;
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
		OptionDef group = OptionDef.builder().longName("group").name("g").hasArg(true).required(true)
				.valueParser("toString").build();
		schema.addOptionDefinitions(group);
		schema.addInputSchema(new BindingPort(ALL_STUDENT, JavaRDD.class, "Student"));
		schema.addOutputSchema(new BindingPort(FILTERED_STUDENT, JavaRDD.class, "Student"));
	}

	public AgeFilter(JavaSparkContext ctx, ParsedOptions options) {
		super(ctx, options);
	}

	@Override
	public Schema getSchema() {
		return schema;
	}

	@Override
	public void exec() {
		@SuppressWarnings("unchecked")
		JavaRDD<Student> allStudents = (JavaRDD<Student>) getInputValue(ALL_STUDENT);
		JavaRDD<Student> output = getStudentOlderThan17(allStudents);
		addOutputValue(FILTERED_STUDENT, output);
	}
	
	public static JavaRDD<Student> getStudentOlderThan17(JavaRDD<Student> allStudents) {
		return allStudents.filter(s -> s.getAge() > 17);
	}

}