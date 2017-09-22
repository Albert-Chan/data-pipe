package com.dataminer.module;

import java.util.List;

import com.dataminer.framework.pipeline.PipelineContext;
import com.dataminer.util.MultiTypeMap;

@Deprecated
public class ModuleFactory {

	public static Module createModule(String moduleName, PipelineContext context) {
		// get schema
		MultiTypeMap schema = ModuleRegistry.getModuleSchema(moduleName);
		// get Option defs
		List<String> defs = ModuleRegistry.getModuleOptionDefs(moduleName);
		
		// instantiate the module
		Module m;
		try {
			m = (Module) Class.forName(moduleName).newInstance();
		} catch (InstantiationException | IllegalAccessException | ClassNotFoundException e) {
			return null;
		}
		m.setContext(context);
//		m.setSchema(schema);
//		m.setOptionDefs(defs);
		return m;
	}

//	/**
//	 * Creates a producer module, normally crawls an outer data source as input.
//	 * 
//	 * @param moduleName
//	 * @return
//	 */
//	public static Module createModule(String moduleName, InOutBinding config) {
//		try {
//			return (Module) Class.forName(moduleName).newInstance();
//		} catch (InstantiationException | IllegalAccessException | ClassNotFoundException e) {
//			e.printStackTrace();
//			return null;
//		}
//	}
//
//	public static Module createModule(String moduleName, Module parent, InOutBinding config) {
//		try {
//			return (Module) Class.forName(moduleName).newInstance();
//		} catch (InstantiationException | IllegalAccessException | ClassNotFoundException e) {
//			// TODO Auto-generated catch block
//			e.printStackTrace();
//			return null;
//		}
//	}

}
