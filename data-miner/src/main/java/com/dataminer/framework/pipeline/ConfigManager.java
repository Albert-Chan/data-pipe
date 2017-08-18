package com.dataminer.framework.pipeline;

import java.util.List;

import com.dataminer.configuration.OptionsParser;
import com.dataminer.configuration.OptionsParser.OptionsParseException;
import com.dataminer.configuration.OptionsParser.OptionsParserBuildException;
import com.dataminer.configuration.ParsedOptions;
import com.dataminer.util.PassengerConfig;

public class ConfigManager {

	private OptionsParser parser;

	private PassengerConfig psgConf = PassengerConfig.getConfig();;

	public ConfigManager() {
		
	}

	public OptionsParser createOptionsParser(List<String> optDefs) throws OptionsParserBuildException {
		parser = new OptionsParser(optDefs);
		return parser;
	}

	public ParsedOptions parseArgs(String[] args) throws OptionsParseException {
		return parser.parse(args);
	}

	public Object get(String propName) {
		return null;
	}
}
