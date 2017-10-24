package com.dataminer.example.module;

import java.util.Arrays;
import java.util.List;

import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

import com.dataminer.configuration.options.ParsedOptions;
import com.dataminer.framework.pipeline.PipelineContext;
import com.dataminer.module.Module;
import com.dataminer.schema.Schema;
import com.dataminer.schema.Schema.BindingPort;

public class StudentLocation extends Module {

	public static final String STUDENT_COUNTRY = "studentCountry";
	public static Schema schema = new Schema();
	static {
		prepareSchema();
	}

	public StudentLocation(String[] args, PipelineContext context) {
		super(args, context);
	}
	
	@Override
	public Schema getSchema() {
		return schema;
	}

	public static void prepareSchema() {
		List<String> optionDef = Arrays.asList("g,	group,	hasArg, required, , toString,	The application group",
				"i, input,	hasArg, required, , toString,	The HDFS input path");

		schema.addOptionsDefinition(optionDef);
		schema.addOutputSchema(new BindingPort(STUDENT_COUNTRY, JavaRDD.class, "StudentCountry"));
	}

	@Override
	public void exec(ParsedOptions parsedOptions) {
		String input = parsedOptions.get("input");
		System.out.println(input);
		// JavaRDD<String> output = context.textFile(input);

		JavaRDD<StudentCountry> output = context.getJavaSparkContext().parallelize(Arrays.asList("S1,USA", "S2,CHN", "S3,GER")).map(line -> {
			String[] attrs = line.split(",");
			return new StudentCountry(attrs[0], attrs[1]);
		});

		addOutputValue(STUDENT_COUNTRY, output);
	}
	
	public static JavaRDD<StudentCountry> source(JavaSparkContext context) {
		return context.parallelize(Arrays.asList("S1,USA", "S2,CHN", "S3,GER")).map(line -> {
			String[] attrs = line.split(",");
			return new StudentCountry(attrs[0], attrs[1]);
		});
	}

}