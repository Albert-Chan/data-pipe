package com.dataminer.example.module;

import org.apache.spark.api.java.JavaRDD;

import com.dataminer.configuration.modules.ModuleConfig;
import com.dataminer.configuration.options.OptionsParser;
import com.dataminer.configuration.options.OptionsParser.OptionsParseException;
import com.dataminer.configuration.options.OptionsParser.OptionsParserBuildException;
import com.dataminer.configuration.options.ParsedOptions;
import com.dataminer.framework.pipeline.PipelineContext;
import com.dataminer.module.Module;
import com.dataminer.util.MultiTypeMap;

public class SomeInputModule extends Module {	
	
	protected PipelineContext context;
	protected ModuleConfig config;

	protected JavaRDD<String> output;
	MultiTypeMap outputs = new MultiTypeMap();

	public SomeInputModule(ModuleConfig config) {
		this.config = config;
	}

	public boolean validate() {
		return true;
	}

	public void exec() throws OptionsParserBuildException, OptionsParseException {
		if (validate() && rerunIfExist) {
			String input;
			try {
				OptionsParser parser = new OptionsParser(SomeInputModuleSchema.getOptionsDefinition());
				ParsedOptions parsedOptions = parser.parse(config.getOptions());
				input = parsedOptions.get("input");
				output = ctx.textFile(input);
			} catch (OptionsParserBuildException e) {
				
			} catch (OptionsParseException e) {
				
			}
		}
	}

	public <T> JavaRDD<T> getOutputByName(String name) {
		
		
		
		
	}

}