package com.dataminer.module;

import com.dataminer.configuration.options.ParsedOptions;

public class CompositedModule extends Module {
	private Module[] modules = null;

	public CompositedModule(Module... modules) {
		this.modules = modules;
	}

	public void exec(ParsedOptions options) {
		return;
	}

}
