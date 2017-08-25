package com.dataminer.module;

import org.apache.spark.api.java.JavaSparkContext;

import com.dataminer.configuration.modules.ModuleConfig;
import com.dataminer.util.MultiTypeMap;

public abstract class Module {

	protected JavaSparkContext ctx;
	protected Module parent;

	protected MultiTypeMap schema;
	protected String[] optionDefs;
	// protected boolean rerunIfExist;

	protected ModuleConfig config;

	public JavaSparkContext getContext() {
		return ctx;
	}

	public void setContext(JavaSparkContext ctx) {
		this.ctx = ctx;
	}

	public Module getParent() {
		return parent;
	}

	public void setParent(Module parent) {
		this.parent = parent;
	}

	public MultiTypeMap getSchema() {
		return schema;
	}

	public void setSchema(MultiTypeMap schema) {
		this.schema = schema;
	}

	public String[] getOptionDefs() {
		return optionDefs;
	}

	public void setOptionDefs(String[] optionDefs) {
		this.optionDefs = optionDefs;
	}

	public ModuleConfig getConfig() {
		return config;
	}

	public abstract void exec(ModuleConfig config) throws Exception;

	public void sink() {

	}

}
