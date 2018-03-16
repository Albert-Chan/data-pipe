package com.dataminer.example.cps;

import java.io.Serializable;
import java.util.Arrays;
import java.util.List;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

public class Example {

	private static final JavaSparkContext ctx = createContext();
	private static JavaSparkContext createContext() {
		SparkConf sparkConf = new SparkConf().setMaster("local").setAppName("example");
		JavaSparkContext ctx = new JavaSparkContext(sparkConf);
		return ctx;
	}
	
	public static JavaSparkContext getContext() {
		return ctx;
	}

	static class InputReader implements SourceModuleFunction<Student> {
		private InterimModuleFunction<Student> next;

		public InputReader(InterimModuleFunction<Student> next) {
			this.next = next;
		}

		public void apply() {
			JavaRDD<Student> output = getContext().parallelize(Arrays.asList("S1,17", "S2,18", "S3,23")).map(line -> {
				String[] attrs = line.split(",");
				return new Student(attrs[0], Integer.parseInt(attrs[1]));
			});
			next.apply(output);
		}
	}

	static class AgeFilter implements InterimModuleFunction<Student> {
		private SinkModuleFunction<Student> next;

		public AgeFilter(SinkModuleFunction<Student> next) {
			this.next = next;
		}

		public void apply(JavaRDD<Student> input) {
			JavaRDD<Student> result = input.filter(s -> s.getAge() > 17);
			this.next.apply(result);
		}
	}

	public static SinkModuleFunction<Student> sink = (JavaRDD<Student> s) -> {
		List<Student> filtered = s.collect();
		System.out.println(s);
		filtered.toString();
		System.out.println(filtered.toString());
	};
	
	public static void main(String[] args) {		
		new InputReader(new AgeFilter(sink)).apply();
	}

}

class Student implements Serializable {
	private static final long serialVersionUID = -3167129622840426500L;

	private String name;
	private int age;
	
	public Student(String name, int age) {
		this.name = name;
		this.age = age;
	}
	
	public String getName() {
		return name;
	}

	public int getAge() {
		return age;
	}

	public String toString() {
		return name + "," + age;
	}

}