package com.dataminer.configuration.modules;

import java.util.HashMap;
import java.util.Map;

import com.dataminer.module.Module;

/**
 * handle input/output binding
 *
 */
public class InOutBinding {

	/**
	 * key: current module input stub, value: parent module output data
	 */
	private Map<String, OutputDock> binding = new HashMap<>();

	public void setInput(String input, Module outputModule, String outputDock) {
		binding.put(input, new OutputDock(outputModule, outputDock));
	}

	public OutputDock getInput(String input) {
		return binding.get(input);
	}

	class OutputDock {
		Module outputModule;
		String outtputDockName;

		OutputDock(Module module, String dockerName) {
			this.outputModule = module;
			this.outtputDockName = dockerName;
		}
	}

}
