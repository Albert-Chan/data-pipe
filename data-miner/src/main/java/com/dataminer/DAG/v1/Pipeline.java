package com.dataminer.DAG.v1;

import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import com.dataminer.DAG.v1.module.Module;
import com.dataminer.DAG.v1.module.ModuleFactory;
import com.dataminer.DAG.v1.module.ModuleFactory.ModuleCreationException;
import com.dataminer.DAG.v1.module.SinkModule;

public class Pipeline {
	private PipelineContext context;

	private Set<SinkModule> sinkers = new HashSet<>();

	public Pipeline(String pipeName) {
		this.context = new PipelineContext(pipeName);
	}

	public <T extends Module> T createModule(Class<T> moduleName, String[] args) throws ModuleCreationException {
		T module = ModuleFactory.create(moduleName, args, context);
		if (module instanceof SinkModule) {
			sinkers.add((SinkModule) module);
		}
		return module;
	}

	public <T extends Module> T createModule(Class<T> moduleName, Map<String, Object> props)
			throws ModuleCreationException {
		T module = ModuleFactory.create(moduleName, props, context);
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
