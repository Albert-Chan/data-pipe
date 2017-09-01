package com.dataminer.module;

import java.util.HashMap;
import java.util.List;
import java.util.function.Function;

import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

import com.dataminer.configuration.modules.InOutBinding;
import com.dataminer.configuration.options.OptionsParser;
import com.dataminer.configuration.options.OptionsParser.OptionsParseException;
import com.dataminer.configuration.options.OptionsParser.OptionsParserBuildException;
import com.dataminer.configuration.options.ParsedOptions;
import com.dataminer.framework.pipeline.Pipeline;
import com.dataminer.util.MultiTypeMap;
import com.dataminer.util.MultiTypeMap2;
import com.dataminer.util.MultiTypeMap2.Key;

public abstract class Module {

	protected String name;
	protected JavaSparkContext ctx;
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

	// protected ModuleConfig config;
	//
	// public ModuleConfig getConfig() {
	// return config;
	// }
	
	
	public String getName() {
		return name;
	}
	

	public JavaSparkContext getContext() {
		return ctx;
	}

	public void setContext(JavaSparkContext ctx) {
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

	public final void doTask(String[] args) {
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

	public abstract boolean validate();

	public abstract void exec(ParsedOptions options) throws Exception;

	public void sink() {

	}
	
	
	public Pipeline fork() {
		return null;
	}

	public Module combine(Module... modules) {
		return null;
	}

	public Module forward(String moduleName, String[] args, Function<> binding) {
		Module next = ModuleFactory.createModule(moduleName, context);

		InOutBinding binding = new InOutBinding();
		binding.setInput(.getOutputRDD());
		binding.setOutput();

		try {
			next.doTask(args);
		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}

		return next;
	}
	
	
	

}
