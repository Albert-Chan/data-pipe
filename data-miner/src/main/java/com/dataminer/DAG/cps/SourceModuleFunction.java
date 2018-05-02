package com.dataminer.DAG.cps;

@FunctionalInterface
public interface SourceModuleFunction<T> {
	void apply();
}
