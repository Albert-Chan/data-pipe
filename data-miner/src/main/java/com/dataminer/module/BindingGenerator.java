package com.dataminer.module;

@FunctionalInterface
public interface BindingGenerator {
	void bind(Module injector, Module acceptor);
}
