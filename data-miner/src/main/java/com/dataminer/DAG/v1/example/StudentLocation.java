package com.dataminer.DAG.v1.example;

import java.util.Arrays;

import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

import com.dataminer.configuration.options.OptionDef;
import com.dataminer.configuration.options.ParsedOptions;
import com.dataminer.DAG.v1.Schema;
import com.dataminer.DAG.v1.Schema.BindingPort;
import com.dataminer.DAG.v1.example.pojo.StudentCountry;
import com.dataminer.DAG.v1.module.Module;

public class StudentLocation extends Module {

	public static final String STUDENT_COUNTRY = "studentCountry";
	public static Schema schema = new Schema();
	static {
		prepareSchema();
	}

	public StudentLocation(JavaSparkContext ctx, ParsedOptions options) {
		super(ctx, options);
	}

	@Override
	public Schema getSchema() {
		return schema;
	}

	public static void prepareSchema() {
		OptionDef group = OptionDef.builder().longName("group").name("g").hasArg(true).required(true)
				.valueParser("toString").build();
		OptionDef input = OptionDef.builder().longName("input").name("i").hasArg(true).required(true)
				.valueParser("toString").description("The HDFS input path").build();
		schema.addOptionDefinitions(group, input);

		schema.addOutputSchema(new BindingPort(STUDENT_COUNTRY, JavaRDD.class, "StudentCountry"));
	}

	@Override
	public void exec() {
		String input = options.get("input");
		System.out.println(input);
		// JavaRDD<String> output = context.textFile(input);

		JavaRDD<StudentCountry> output = context.getJavaSparkContext()
				.parallelize(Arrays.asList("S1,USA", "S2,CHN", "S3,GER")).map(line -> {
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