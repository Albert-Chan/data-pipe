package com.dataminer.example.module;

import java.util.Arrays;
import java.util.List;

import org.apache.spark.api.java.JavaRDD;

import com.dataminer.configuration.options.ParsedOptions;
import com.dataminer.framework.pipeline.PipelineContext;
import com.dataminer.module.Module;
import com.dataminer.schema.Schema;
import com.dataminer.schema.Schema.BindingPort;

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
		List<String> optionDef = Arrays.asList("g,	group,	hasArg,	required, , toString, The application group name");
		schema.addOptionsDefinition(optionDef);
		schema.addInputSchema(new BindingPort(FILTERED_STUDENT, JavaRDD.class, "Student"));
		schema.addInputSchema(new BindingPort(STUDENT_COUNTRY, JavaRDD.class, "studentCountry"));

		schema.addOutputSchema(new BindingPort(FILTERED_STUDENT_WITH_COUNTRY, JavaRDD.class, "StudentFullProperties"));
	}

	public RDDJoin(String[] args, PipelineContext context) {
		super(args, context);
	}

	@Override
	public Schema getSchema() {
		return schema;
	}

	@Override
	public void exec(ParsedOptions parsedOptions) {
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