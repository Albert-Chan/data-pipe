package com.dataminer.configuration;

import java.util.List;

import com.dataminer.configuration.options.OptionsParser;
import com.dataminer.configuration.options.ParsedOptions;
import com.dataminer.configuration.options.OptionsParser.OptionsParseException;
import com.dataminer.configuration.options.OptionsParser.OptionsParserBuildException;
import com.dataminer.util.PassengerConfig;

public class ConfigManager {

	private PassengerConfig psgConf = PassengerConfig.getConfig();

	public ConfigManager() {
		
	}


	public Schema createSchemaParser(String[] args) {
		return parser.parse(args);
	}
	
	
	
}
