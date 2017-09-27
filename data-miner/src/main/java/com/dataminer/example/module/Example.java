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
			Collector collector = pipe.createModule(Collector.class, new String[] { "-g", "test" });
		
			studentGen.bind("hdfsInput", hdfsReader, "hdfsOutput");
			ageFilter.bind("allStudent", studentGen, "allStudent");
			collector.bind("filteredStudent", ageFilter, "filteredStudent");
			
		} catch (ModuleCreationException e) {
			// logger ...
			return;
		}

		System.out.println(pipe.getMessageCombiner().event("action", "appStart").getMessage());
		pipe.run();
		
		// TRIGGER.send(mc.event("action", "appEnd"));
		System.out.println(pipe.getMessageCombiner().event("action", "appEnd").getMessage());
	}

}
