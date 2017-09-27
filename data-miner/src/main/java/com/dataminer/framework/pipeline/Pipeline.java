package com.dataminer.framework.pipeline;

import java.util.HashSet;
import java.util.Set;

import org.apache.spark.api.java.JavaSparkContext;

import com.dataminer.module.Module;
import com.dataminer.module.ModuleFactory;
import com.dataminer.module.ModuleFactory.ModuleCreationException;
import com.dataminer.module.SinkModule;
import com.dataminer.monitor.AppEventTrigger;
import com.dataminer.monitor.MessageCombiner;

public class Pipeline {	
	private PipelineContext context;
	private static final AppEventTrigger TRIGGER = AppEventTrigger.get();
	private MessageCombiner mc = new MessageCombiner();
	
	private Set<SinkModule> sinkers = new HashSet<>();

	public Pipeline(String pipeName) {
		this.context = new PipelineContext(pipeName);
		mc.partOfKey("appId", JavaSparkContext.toSparkContext(context.getJavaSparkContext()).applicationId());
		mc.partOfKey("pipeName", "pipeline example");
	}
	
	public MessageCombiner getMessageCombiner() {
		return mc;
	}

	public <T extends Module> T createModule(Class<T> moduleName, String[] args)
			throws ModuleCreationException {
		T module = ModuleFactory.create(moduleName, args, context);
		if (module instanceof SinkModule) {
			sinkers.add((SinkModule) module);
		}
		return module;
	}

	public void run() {
		for (SinkModule sink : sinkers) {
			sink.doTask();
		}
		context.close();
		TRIGGER.close();
	}
	
}
