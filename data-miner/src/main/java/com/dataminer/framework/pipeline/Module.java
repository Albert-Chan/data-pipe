package com.dataminer.framework.pipeline;

import org.apache.spark.api.java.JavaSparkContext;

public abstract class Module {
	protected JavaSparkContext ctx;
	protected Module parent;
	
	protected boolean rerunIfExist;
	public abstract void exec();
	
	
	
	private void exec() {
		if (validate() && rerunIfExist) {
			String path = inputFilePath + analyticDay.formatTime("yyyy/MM/dd");
			output = ctx.textFile(path);
		}
	}
	
	
	public Module forward(String moduleName) {
		Module next = ModuleFactory.createModule( String moduleName, Module parent);
		next.exec();
		return next;
	}
	
	public void to() {
		
	}
	
}
