package com.dataminer.framework.pipeline;

import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import com.dataminer.module.Module;
import com.dataminer.module.ModuleFactory;
import com.dataminer.module.ModuleFactory.ModuleCreationException;
import com.dataminer.module.SinkModule;

public class Pipeline {	
	private PipelineContext context;
	
	private Set<SinkModule> sinkers = new HashSet<>();

	public Pipeline(String pipeName) {
		this.context = new PipelineContext(pipeName);
	}
	
	public <T extends Module> T createModule(Class<T> moduleName, String[] args)
			throws ModuleCreationException {
		T module = ModuleFactory.create(moduleName, args, context);
		if (module instanceof SinkModule) {
			sinkers.add((SinkModule) module);
		}
		return module;
	}
	
	public <T extends Module> T createModule(Class<T> moduleName, Map options)
			throws ModuleCreationException {
		T module = ModuleFactory.create(moduleName, options, context);
		if (module instanceof SinkModule) {
			sinkers.add((SinkModule) module);
		}
		return module;
	}

	public void run() throws Exception {
		for (SinkModule sink : sinkers) {
			sink.doTask();
		}
		context.close();
	}
	
}
