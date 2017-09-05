package com.dataminer.module;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.spark.api.java.JavaRDD;

import com.dataminer.configuration.options.OptionsParser;
import com.dataminer.configuration.options.OptionsParser.OptionsParseException;
import com.dataminer.configuration.options.OptionsParser.OptionsParserBuildException;
import com.dataminer.configuration.options.ParsedOptions;
import com.dataminer.framework.pipeline.Pipeline;
import com.dataminer.framework.pipeline.PipelineContext;
import com.dataminer.util.MultiTypeMap;
import com.dataminer.util.MultiTypeMap2;
import com.dataminer.util.MultiTypeMap2.Key;

public abstract class Module {

	protected String name;
	protected PipelineContext ctx;
	protected HashMap<String, Module> parents;

	protected MultiTypeMap schema;
	protected List<String> optionDefs;

	protected boolean rerunIfExist = true;

	private MultiTypeMap2<JavaRDD<?>> rddOutputs = new MultiTypeMap2<>();

	@SuppressWarnings("unchecked")
	public <T> JavaRDD<T> getOutputRDD(String name, Class<T> type) {
		return (JavaRDD<T>) rddOutputs.get(new Key<T>(name, type));
	}

	public <T> void addOutputRDD(String name, Class<T> type, JavaRDD<T> rdd) {
		rddOutputs.put(new Key<T>(name, type), rdd);
	}

	public String getName() {
		return name;
	}

	public PipelineContext getContext() {
		return ctx;
	}

	public void setContext(PipelineContext ctx) {
		this.ctx = ctx;
	}

	public Module getParent(String name) {
		return parents.get(name);
	}

	public void setParent(Module parent) {
		parents.put(parent.getName(), parent);
	}

	public MultiTypeMap getSchema() {
		return schema;
	}

	public void setSchema(MultiTypeMap schema) {
		this.schema = schema;
	}

	public List<String> getOptionDefs() {
		return optionDefs;
	}

	public void setOptionDefs(List<String> optionDefs) {
		this.optionDefs = optionDefs;
	}

	/**
	 * A map from current module(acceptor) input stub to upstream module(injector)
	 * output stub(injector stub).
	 */
	private Map<String, InputDescription> binding = new HashMap<>();

	public void setInput(String acceptorStub, Module injector, String injectorStub) {
		binding.put(acceptorStub, new InputDescription(injector, injectorStub));
	}

	public InputDescription getInput(String acceptorStub) {
		return binding.get(acceptorStub);
	}

	static class InputDescription {
		Module injector;
		String injectorStub;

		InputDescription(Module module, String injectorStub) {
			this.injector = module;
			this.injectorStub = injectorStub;
		}
	}

	protected void doTask(String[] args) {
		if (validate() && rerunIfExist) {
			try {
				OptionsParser parser = new OptionsParser(optionDefs);
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

	public abstract void exec(ParsedOptions options) throws Exception;

	public void sink() {

	}

	public Pipeline fork() {
		return null;
	}

	public Module combine(Module... modules) {
		return null;
	}

	public Module forward(PipelineContext context, String moduleName, String[] args, BindingGenerator gen) {
		Module next = ModuleFactory.createModule(moduleName, context);
		gen.bind(this, next);

		try {
			next.doTask(args);
		} catch (Exception e) {
			e.printStackTrace();
		}
		return next;
	}

}
