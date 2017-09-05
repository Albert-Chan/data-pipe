package com.dataminer.framework.pipeline;

import com.dataminer.module.Module;

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

}
