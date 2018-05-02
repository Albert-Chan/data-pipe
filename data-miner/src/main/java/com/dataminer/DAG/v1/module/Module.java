package com.dataminer.DAG.v1.module;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

import org.apache.spark.api.java.JavaSparkContext;

import com.dataminer.DAG.v1.PipelineContext;
import com.dataminer.DAG.v1.Schema;
import com.dataminer.DAG.v1.Schema.BindingPort;
import com.dataminer.configuration.options.Options;
import com.dataminer.configuration.options.OptionsParser.OptionsParseException;
import com.dataminer.configuration.options.OptionsParser.OptionsParserBuildException;
import com.dataminer.monitor.MonitorContext;
import com.dataminer.configuration.options.ParsedOptions;

/**
 * The inputSchema and outputSchema will not be in the options.
 */
public abstract class Module {
	protected String name;
	protected List<Module> parents = new ArrayList<>();

	protected boolean finished = false;

	protected abstract Schema getSchema();

	protected List<String> optionDefs;
	protected Options optionSchema;

	protected ParsedOptions options;

	protected PipelineContext context;
	protected JavaSparkContext ctx;
	protected MonitorContext monitorContext;

	public Module(JavaSparkContext ctx, String[] args) throws OptionsParserBuildException, OptionsParseException {
		this.ctx = ctx;
		this.optionSchema = Options.define(optionDefs);
		this.options = optionSchema.parse(args);
		this.monitorContext = genMonitorContext(ctx, options);
	}

	public Module(JavaSparkContext ctx, Map<String, Object> props) {
		this.ctx = ctx;
		this.monitorContext = genMonitorContext(ctx, options);
	}

	public Module(JavaSparkContext ctx, ParsedOptions options) {
		this.ctx = ctx;
		this.options = options;
		this.monitorContext = genMonitorContext(ctx, options);
	}

	protected MonitorContext genMonitorContext(JavaSparkContext ctx, ParsedOptions options) {
		return MonitorContext.of(JavaSparkContext.toSparkContext(ctx).applicationId(), this.getClass().getName(), null);
	}

	protected MonitorContext genMonitorContext(JavaSparkContext ctx, Map<String, Object> props) {
		return MonitorContext.of(JavaSparkContext.toSparkContext(ctx).applicationId(), this.getClass().getName(),
				props);
	}

	public void addParent(Module parent) {
		parents.add(parent);
	}

	public List<Module> getParents() {
		return parents;
	}

	public String getName() {
		return name;
	}

	private Map<String, ModuleBindingPort> binding = new HashMap<>();

	class ModuleBindingPort {
		Module module;
		String bindingPortName;

		ModuleBindingPort(Module module, String bp) {
			this.module = module;
			this.bindingPortName = bp;
		}
	}

	/**
	 * A map from current module(acceptor) input stub to the actual input value
	 */
	private Map<String, Object> actualBinding = new HashMap<>();

	public Object getInputValue(String acceptorStub) {
		return actualBinding.get(acceptorStub);
	}

	public void setInputValue(String acceptorStub, Module producerModule, String outputName) {
		actualBinding.put(acceptorStub, producerModule.getOutputValue(outputName));
	}

	/**
	 * output name -> value map
	 */
	private Map<String, Object> outputs = new HashMap<>();

	public Object getOutputValue(String name) {
		return outputs.get(name);
	}

	public void addOutputValue(String name, Object value) {
		outputs.put(name, value);
	}

	public void bind(String localName, Module remoteModule, String remoteName) {
		this.addParent(remoteModule);
		// add binding
		binding.put(localName, new ModuleBindingPort(remoteModule, remoteName));
	}

	public void valueBind() {
		// add binding
		for (Entry<String, ModuleBindingPort> e : binding.entrySet()) {
			ModuleBindingPort mbp = e.getValue();
			actualBinding.put(e.getKey(), mbp.module.getOutputValue(mbp.bindingPortName));
		}
	}

	public void doTask() throws Exception {
		if (finished) {
			return;
		}
		for (Module m : parents) {
			m.doTask();
		}
		valueBind();
		if (validate()) {
			exec();
		}
	}

	public boolean validate() {
		// try {
		// 		parsedOptions = Options.of(getSchema().getOptionDefinitions()).parse(args);
		// } catch (OptionsParserBuildException | OptionsParseException e) {
		//		// logger...
		// 		return false;
		// }

		Map<String, BindingPort> inputSchemas = getSchema().getInputSchemas();
		for (String name : inputSchemas.keySet()) {
			BindingPort bp = inputSchemas.get(name);
			Object inputValue = getInputValue(bp.name);
			if (!(bp.type.isInstance(inputValue))) {
				// logger...
				return false;
			}
		}
		return true;
	}

	protected abstract void exec() throws Exception;

}
