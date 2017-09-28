package com.dataminer.schema;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class Schema {
	private List<String> optionDef;

	public List<String> getOptionsDefinition() {
		return optionDef;
	}

	public void addOptionsDefinition(List<String> optionDef) {
		this.optionDef = optionDef;
	}

	private Map<String, BindingPort> input = new HashMap<>();
	private Map<String, BindingPort> output = new HashMap<>();
	
	public void addInputSchema(BindingPort bp) {
		input.put(bp.name, bp);
	}
	
	public Map<String, BindingPort> getInputSchemas() {
		return input;
	}

	public int getInputSchemaSize() {
		return input.size();
	}
	
	public BindingPort getInputSchema(String name) {
		return input.get(name);
	}
		
	public void addOutputSchema(BindingPort bp) {
		output.put(bp.name, bp);
	}

	public BindingPort getOutputSchema(String name) {
		return output.get(name);
	}

	public static class BindingPort {
		public String name;
		/**
		 * JavaRDD, JavaPairRDD, DataFrame
		 */
		public Class type;
		/**
		 * the T of JavaRDD<T>, K, V of JavaPairRDD<K,V>; or the schema of DataFrame
		 */
		public String valueType;

		public BindingPort(String name, Class type, String valueType) {
			this.name = name;
			this.type = type;
			this.valueType = valueType;
		}
	}

}
