package com.dataminer.example.module;

import java.util.Map;

import com.dataminer.framework.pipeline.Pipeline;
import com.dataminer.module.ModuleFactory.ModuleCreationException;

public class Example {

	public static void main(String[] args) throws Exception {

		Pipeline pipe = new Pipeline("example");

		try {
			MockRDD mockRdd = pipe.createModule(MockRDD.class, Map.of("group", "test"));
			StudentGenerator studentGen = pipe.createModule(StudentGenerator.class, Map.of("group", "test"));
			AgeFilter ageFilter = pipe.createModule(AgeFilter.class, Map.of("group", "test"));

			StudentLocation studentLoc = pipe.createModule(StudentLocation.class,
					Map.of("group", "test", "input", "the input path"));
			RDDJoin join = pipe.createModule(RDDJoin.class, Map.of("group", "test"));

			Collector collector = pipe.createModule(Collector.class, Map.of("group", "test"));

			studentGen.bind(StudentGenerator.HDFS_INPUT, mockRdd, MockRDD.OUTPUT);
			ageFilter.bind(AgeFilter.ALL_STUDENT, studentGen, StudentGenerator.ALL_STUDENT);

			join.bind(RDDJoin.FILTERED_STUDENT, ageFilter, AgeFilter.FILTERED_STUDENT);
			join.bind(RDDJoin.STUDENT_COUNTRY, studentLoc, StudentLocation.STUDENT_COUNTRY);

			collector.bind(Collector.OUTPUT_STUDENT, join, RDDJoin.FILTERED_STUDENT_WITH_COUNTRY);

		} catch (ModuleCreationException e) {
			e.printStackTrace();
			// logger ...
			return;
		}
		pipe.run();
	}

}
