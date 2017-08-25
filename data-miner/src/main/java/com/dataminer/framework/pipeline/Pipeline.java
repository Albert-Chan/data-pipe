package com.dataminer.framework.pipeline;

import com.dataminer.configuration.modules.ModuleConfig;
import com.dataminer.module.Module;
import com.dataminer.module.ModuleFactory;

public abstract class Pipeline {
	PipelineContext context;

	public Pipeline(String pipeName) {
		context = new PipelineContext(pipeName);
	}

	public abstract void run();

	protected void start(String[] args) {
		context.init();
	}

	protected void end() {
		context.stop();
	}

	protected void from() {

	}

	public Pipeline forward(String moduleName, String[] args) {
		Module next = ModuleFactory.createModule(moduleName, context);
		ModuleConfig config = new ModuleConfig();
		config.setInput(next.getParent().getOutput());
		config.set
		try {
			next.exec(config);
		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}

		return this;
	}
}
