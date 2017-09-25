package com.dataminer.module;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

import com.dataminer.configuration.options.OptionsParser;
import com.dataminer.configuration.options.OptionsParser.OptionsParseException;
import com.dataminer.configuration.options.OptionsParser.OptionsParserBuildException;
import com.dataminer.configuration.options.ParsedOptions;
import com.dataminer.framework.pipeline.Context;
import com.dataminer.schema.Schema;

public abstract class Module {

	protected String name;

	protected abstract Schema getSchema();

	protected Context context;
	protected ParsedOptions parsedOptions;

	protected boolean finished = false;

	protected List<Module> parents = new ArrayList<>();

	protected String[] args;

	public void addParent(Module parent) {
		parents.add(parent);
	}

	public List<Module> getParents() {
		return parents;
	}

	public String getName() {
		return name;
	}

	public Module(String[] args, Context context) {
		this.context = context;
		this.args = args;
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

	public void actualBind() {
		// add binding
		for (Entry<String, ModuleBindingPort> e : binding.entrySet()) {
			ModuleBindingPort mbp = e.getValue();
			actualBinding.put(e.getKey(), mbp.module.getOutputValue(mbp.bindingPortName));
		}
	}

	public void doTask() {
		if (finished) {
			return;
		}
		if (validate()) {
			for (Module m : parents) {
				m.doTask();
			}
			actualBind();
			exec(parsedOptions);
		}
	}

	public boolean validate() {
		try {
			OptionsParser parser = new OptionsParser(getSchema().getOptionsDefinition());
			parsedOptions = parser.parse(args);
		} catch (OptionsParserBuildException e) {
			e.printStackTrace();
			return false;
		} catch (OptionsParseException e) {
			e.printStackTrace();
			return false;
		} catch (Exception e) {
			e.printStackTrace();
			return false;
		}

		return true;
	}

	protected abstract void exec(ParsedOptions options);

}
