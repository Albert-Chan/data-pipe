package com.dataminer.module;

import java.lang.reflect.Constructor;
import java.lang.reflect.InvocationTargetException;

import com.dataminer.framework.pipeline.Context;

public class ModuleFactory {
	public static <T extends Module> T create(Class<T> moduleName, String[] args, Context context)
			throws ModuleCreationException {
		try {
		Constructor<T> constructor = moduleName.getConstructor(String[].class, Context.class);
		return constructor.newInstance(args, context);}
		catch(InstantiationException| IllegalAccessException| IllegalArgumentException| InvocationTargetException|
				NoSuchMethodException| SecurityException e) {
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
