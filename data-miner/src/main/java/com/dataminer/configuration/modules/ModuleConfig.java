package com.dataminer.configuration.modules;

import com.dataminer.configuration.options.OptionDef;
import com.dataminer.configuration.options.OptionsParser;

public class ModuleConfig {
	private String[] args;
//	Map<String, Class<?>> input = new HashMap<>();
//	Map<String, Class<?>> output = new HashMap<>();

//	public String[] getOptions() {
//		return options;
//	}
//
//	public void setOptions(String[] options) {
//		this.options = options;
//	}

	ModuleConfig(OptionDef def) {
		OptionsParser parser = new OptionsParser(def);
		parser.parse(args)
	}
	
	
}
