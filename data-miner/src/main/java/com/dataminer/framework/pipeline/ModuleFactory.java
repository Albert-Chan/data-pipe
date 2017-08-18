package com.dataminer.framework.pipeline;

import com.dataminer.example.ExampleModule;

public class ModuleFactory {
	public static Module createModule( String moduleName ) {
		return new ExampleModule();
	}
}
