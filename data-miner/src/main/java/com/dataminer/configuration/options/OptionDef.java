package com.dataminer.configuration.options;

import org.apache.commons.cli.Option;

public class OptionDef {
	static final String HAS_ARG = "hasArg";
	static final String REQUIRED = "required";
	private Option opt;
	private String parseFunc;
	private String defaultValue;

	/**
	 * Generates the option definition from a comma separated string in the format
	 * of <i>opt, longOpt, hasArg, isRequired, defaultValue, valueParser,
	 * description</i>
	 * 
	 * @param strOptDef
	 * @return Option definition
	 */
	public static OptionDef from(String strOptDef) {
		String[] props = strOptDef.split(",");
		String opt = props[0].trim();
		String longOpt = props[1].trim();
		boolean hasArg = OptionDef.HAS_ARG.equals(props[2].trim());
		String description = props[6].trim();
		Option option = new Option(opt, longOpt, hasArg, description);
		boolean required = OptionDef.REQUIRED.equals(props[3].trim());
		option.setRequired(required);
		String defaultValue = props[4].trim();
		String valueParser = props[5].trim();
		OptionDef optDef = new OptionDef(option, valueParser);
		optDef.setDefaultValue(defaultValue);
		return optDef;
	}

	private OptionDef() {
	}

	private OptionDef(Option opt, String parseFunc) {
		this.opt = opt;
		this.parseFunc = parseFunc;
	}

	public Option getOption() {
		return opt;
	}

	public String getParseFunc() {
		return parseFunc;
	}

	public String getDefaultValue() {
		return defaultValue;
	}

	public void setDefaultValue(String defaultValue) {
		this.defaultValue = defaultValue;
	}

	public static OptionDefBuilder builder() {
		return new OptionDefBuilder();
	}

	public static class OptionDefBuilder {
		String opt;
		String longOpt;
		boolean hasArg;
		String description;
		boolean required;
		String defaultValue;
		String valueParser;

		public OptionDefBuilder name(String name) {
			this.opt = name;
			return this;
		}

		public OptionDefBuilder longName(String longName) {
			this.longOpt = longName;
			return this;
		}

		public OptionDefBuilder hasArg(boolean hasArg) {
			this.hasArg = hasArg;
			return this;
		}

		public OptionDefBuilder description(String description) {
			this.description = description;
			return this;
		}

		public OptionDefBuilder required(boolean required) {
			this.required = required;
			return this;
		}

		public OptionDefBuilder defaultValue(String defaultValue) {
			this.defaultValue = defaultValue;
			return this;
		}

		public OptionDefBuilder valueParser(String valueParser) {
			this.valueParser = valueParser;
			return this;
		}

		public OptionDef build() {
			Option option = new Option(opt, longOpt, hasArg, description);
			option.setRequired(required);
			OptionDef optDef = new OptionDef(option, valueParser);
			optDef.setDefaultValue(defaultValue);
			return optDef;
		}

	}

}