package com.dataminer.framework.pipeline;

import com.dataminer.configuration.modules.InOutBinding;
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

	protected Pipeline from() {
		return null;
	}

	public Pipeline fork() {
		return null;
	}

	public Pipeline parallel(Module... modules) {
		return null;
	}

	public Pipeline forward(String moduleName, String[] args) {
		Module next = ModuleFactory.createModule(moduleName, context);

		InOutBinding binding = new InOutBinding();
		binding.setInput(.getOutputRDD());
		binding.setOutput();

		try {
			next.doTask(args);
		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}

		return this;
	}
}
