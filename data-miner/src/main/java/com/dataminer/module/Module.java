package com.dataminer.module;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.CountDownLatch;

import com.dataminer.configuration.options.OptionsParser;
import com.dataminer.configuration.options.OptionsParser.OptionsParseException;
import com.dataminer.configuration.options.OptionsParser.OptionsParserBuildException;
import com.dataminer.configuration.options.ParsedOptions;
import com.dataminer.framework.pipeline.PipelineContext;
import com.dataminer.schema.Schema;

public abstract class Module {

	protected String name;

	protected Schema schema;

	protected PipelineContext ctx;
	protected boolean rerunIfExist = true;

	public String getName() {
		return name;
	}

	public PipelineContext getContext() {
		return ctx;
	}

	public void setContext(PipelineContext ctx) {
		this.ctx = ctx;
	}
	
	
	public Module(String[] args) {
		prepareSchema();
		bindingDoneSignal = new CountDownLatch(schema.getInputSchemaSize());
		// wait for all bindings to complete
		try {
			bindingDoneSignal.await();
		} catch (InterruptedException e) {
		}
		doTask(args);
	}
	

	/**
	 * A map from current module(acceptor) input stub to the actual input value
	 */
	private Map<String, Object> binding = new HashMap<>();

	private CountDownLatch bindingDoneSignal = null;

	public Object getInputValue(String acceptorStub) {
		return binding.get(acceptorStub);
	}

	public void setInputValue(String acceptorStub, Module producerModule, String outputName) {
		binding.put(acceptorStub, producerModule.getOutputValue(outputName));
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
		// add binding
		binding.put(localName, remoteModule.getOutputValue(remoteName));
		bindingDoneSignal.countDown();
	}

	protected void doTask(String[] args) {
		if (validate() && rerunIfExist) {
			try {
				OptionsParser parser = new OptionsParser(schema.getOptionsDefinition());
				ParsedOptions parsedOptions = parser.parse(args);
				exec(parsedOptions);
			} catch (OptionsParserBuildException e) {

			} catch (OptionsParseException e) {

			} catch (Exception e) {

			}
		}
	}
	
	

	public boolean validate() {
		return true;
	}

	public abstract void prepareSchema();
	
	public abstract void exec(ParsedOptions options) throws Exception;

}
