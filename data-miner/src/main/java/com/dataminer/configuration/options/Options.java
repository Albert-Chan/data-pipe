package com.dataminer.configuration.options;

import java.util.List;

import com.dataminer.configuration.options.OptionsParser.OptionsParseException;
import com.dataminer.configuration.options.OptionsParser.OptionsParserBuildException;

public class Options {

	OptionsParser parser;

	private Options(List<String> optionDefs) throws OptionsParserBuildException {
		this.parser = new OptionsParser(optionDefs);
	}

	private Options(OptionDef... optionDefs) throws OptionsParserBuildException {
		this.parser = new OptionsParser(optionDefs);
	}

	public static Options define(List<String> optionDefs) throws OptionsParserBuildException {
		return new Options(optionDefs);
	}

	public static Options of(OptionDef... defs) throws OptionsParserBuildException {
		return new Options(defs);
	}

	public ParsedOptions parse(String[] args) throws OptionsParseException {
		return parser.parse(args);
	}

}
