package com.dataminer.module;

import java.lang.reflect.Constructor;
import java.lang.reflect.InvocationTargetException;
import java.util.Map;

import com.dataminer.framework.pipeline.PipelineContext;

public class ModuleFactory {
	public static <T extends Module> T create(Class<T> moduleName, String[] args, PipelineContext context)
			throws ModuleCreationException {
		try {
			Constructor<T> constructor = moduleName.getConstructor(String[].class, PipelineContext.class);
			return constructor.newInstance(context, args);
		} catch (InstantiationException | IllegalAccessException | IllegalArgumentException | InvocationTargetException
				| NoSuchMethodException | SecurityException e) {
			throw new ModuleCreationException(e);
		}
	}

	public static <T extends Module> T create(Class<T> moduleName, Map<String, Object> props, PipelineContext context)
			throws ModuleCreationException {
		try {
			Constructor<T> constructor = moduleName.getConstructor(String[].class, PipelineContext.class);
			return constructor.newInstance(context, props);
		} catch (InstantiationException | IllegalAccessException | IllegalArgumentException | InvocationTargetException
				| NoSuchMethodException | SecurityException e) {
			throw new ModuleCreationException(e);
		}
	}

	public static class ModuleCreationException extends Exception {
		private static final long serialVersionUID = 9073903913528451230L;

		public ModuleCreationException(String msg) {
			super(msg);
		}

		public ModuleCreationException(Throwable cause) {
			super(cause);
		}
	}
}
