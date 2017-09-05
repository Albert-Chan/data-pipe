package com.dataminer.configuration.modules;

import org.junit.Test;

import com.dataminer.example.module.SomeInterimModule;
import com.dataminer.example.module.StudentReader;
import com.dataminer.framework.pipeline.PipelineContext;
import com.dataminer.module.BindingGenerator;
import com.dataminer.module.ModuleFactory;

public class InOutBindingTest {

	@Test
	public void test() {

		StudentReader studentReader = (StudentReader) ModuleFactory.createModule("StudentReader",
				new PipelineContext("test Pipe"));
		SomeInterimModule interim = (SomeInterimModule) ModuleFactory.createModule("Interim",
				new PipelineContext("test Pipe"));

		BindingGenerator binding = (m1, m2) -> {
			m2.setInput("student-interim", m1, "Student");
		};
		
		binding.bind(studentReader, interim);

		// InOutBinding binding = new InOutBinding();
		// binding.setInput("student", interim, );
		// binding.setOutputSchema("StringOut", String.class);
		//
		// Class<?> clazz = binding.getInputSchema("String");
		// assertEquals("java.lang.String", clazz.getName());

	}
}
