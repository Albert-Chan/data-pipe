package com.dataminer.DAG.v1.example;

import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

import com.dataminer.configuration.options.OptionDef;
import com.dataminer.configuration.options.ParsedOptions;
import com.dataminer.DAG.v1.Schema;
import com.dataminer.DAG.v1.Schema.BindingPort;
import com.dataminer.DAG.v1.example.pojo.Student;
import com.dataminer.DAG.v1.example.pojo.StudentCountry;
import com.dataminer.DAG.v1.example.pojo.StudentFullProperties;
import com.dataminer.DAG.v1.module.Module;

import scala.Tuple2;

public class RDDJoin extends Module {

	public static final String FILTERED_STUDENT = "filteredStudent";
	public static final String STUDENT_COUNTRY = "studentCountry";

	public static final String FILTERED_STUDENT_WITH_COUNTRY = "filteredStudentWithCountry";

	private static Schema schema = new Schema();
	static {
		prepareSchema();
	}

	public static void prepareSchema() {
		OptionDef group = OptionDef.builder().longName("group").name("g").hasArg(true).required(true)
				.valueParser("toString").build();
		schema.addOptionDefinitions(group);
		schema.addInputSchema(new BindingPort(FILTERED_STUDENT, JavaRDD.class, "Student"));
		schema.addInputSchema(new BindingPort(STUDENT_COUNTRY, JavaRDD.class, "studentCountry"));

		schema.addOutputSchema(new BindingPort(FILTERED_STUDENT_WITH_COUNTRY, JavaRDD.class, "StudentFullProperties"));
	}

	public RDDJoin(JavaSparkContext ctx, ParsedOptions options) {
		super(ctx, options);
	}

	@Override
	public Schema getSchema() {
		return schema;
	}

	@Override
	public void exec() {
		@SuppressWarnings("unchecked")
		JavaRDD<Student> filtered = ((JavaRDD<Student>) getInputValue(FILTERED_STUDENT));
		@SuppressWarnings("unchecked")
		JavaRDD<StudentCountry> country = ((JavaRDD<StudentCountry>) getInputValue(STUDENT_COUNTRY));

		JavaRDD<StudentFullProperties> output = studentPropertyMerge(filtered, country);

		addOutputValue(FILTERED_STUDENT_WITH_COUNTRY, output);
	}

	public static JavaRDD<StudentFullProperties> studentPropertyMerge(JavaRDD<Student> filtered,
			JavaRDD<StudentCountry> country) {
		return filtered.mapToPair(student -> new Tuple2<>(student.getName(), student))
				.join(country.mapToPair(studentCountry -> {
					return new Tuple2<>(studentCountry.getName(), studentCountry);
				})).map(t -> {
					return new StudentFullProperties(t._1, t._2._1.getAge(), t._2._2.getCountry());
				});
	}

}