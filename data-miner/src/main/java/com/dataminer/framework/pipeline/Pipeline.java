package com.dataminer.framework.pipeline;

import com.dataminer.module.Module;

public class Pipeline {
	PipelineContext context;

	public Pipeline(String pipeName) {
		context = new PipelineContext(pipeName);
	}

	public void start(String[] args) {
		context.init(args);
	}

	public void end() {
		context.stop();
	}

	public Pipeline from() {
		return null;
	}

	public Pipeline fork() {
		return null;
	}

	public Pipeline parallel(Module... modules) {
		return null;
	}

}
