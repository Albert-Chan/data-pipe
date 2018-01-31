package com.dataminer.configuration.options;

import java.util.List;

import com.dataminer.configuration.options.OptionsParser.OptionsParseException;
import com.dataminer.configuration.options.OptionsParser.OptionsParserBuildException;

public class Options {

	List<String> optionDefs;
	OptionsParser parser;

	private Options(List<String> optionDefs) throws OptionsParserBuildException {
		this.optionDefs = optionDefs;
		this.parser = new OptionsParser(optionDefs);
	}

	public static Options define(List<String> optionDefs) throws OptionsParserBuildException {
		return new Options(optionDefs);
	}

	public ParsedOptions parse(String[] args) throws OptionsParseException {
		return parser.parse(args);
	}

}
