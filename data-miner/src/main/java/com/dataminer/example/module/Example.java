package com.dataminer.example.module;

import com.dataminer.framework.pipeline.Pipeline;
import com.dataminer.module.ModuleFactory.ModuleCreationException;

public class Example {

	public static void main(String[] args) {
		
		Pipeline pipe = new Pipeline("example");
		
		try {
			HDFSReader hdfsReader = pipe.createModule(HDFSReader.class,
					new String[] { "-g", "test", "-i", "the input path" });
			StudentGenerator studentGen = pipe.createModule(StudentGenerator.class, new String[] { "-g", "test" });
			AgeFilter ageFilter = pipe.createModule(AgeFilter.class, new String[] { "-g", "test" });
			
			StudentLocation studentLoc = pipe.createModule(StudentLocation.class, new String[] { "-g", "test", "-i", "the input path" });
			RDDJoin join = pipe.createModule(RDDJoin.class, new String[] { "-g", "test" });
			
			Collector collector = pipe.createModule(Collector.class, new String[] { "-g", "test" });
		
			studentGen.bind(StudentGenerator.HDFS_INPUT, hdfsReader, HDFSReader.HDFS_OUTPUT);
			ageFilter.bind(AgeFilter.ALL_STUDENT, studentGen, StudentGenerator.ALL_STUDENT);
			
			join.bind(RDDJoin.FILTERED_STUDENT, ageFilter, AgeFilter.FILTERED_STUDENT);
			join.bind(RDDJoin.STUDENT_COUNTRY, studentLoc, StudentLocation.STUDENT_COUNTRY);
			
			
			collector.bind(Collector.OUTPUT_STUDENT, join, RDDJoin.FILTERED_STUDENT_WITH_COUNTRY);
			
		} catch (ModuleCreationException e) {
			e.printStackTrace();
			// logger ...
			return;
		}

		System.out.println(pipe.getMessageCombiner().event("action", "appStart").getMessage());
		pipe.run();
		
		// TRIGGER.send(mc.event("action", "appEnd"));
		System.out.println(pipe.getMessageCombiner().event("action", "appEnd").getMessage());
	}

}
