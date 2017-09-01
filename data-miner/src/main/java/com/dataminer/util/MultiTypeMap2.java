package com.dataminer.util;

import java.util.HashMap;
import java.util.Map;

public class MultiTypeMap2<U> {

	public static class Key<T> {
		final String identifier;
		final Class<T> type;

		public Key(String identifier, Class<T> type) {
			this.identifier = identifier;
			this.type = type;
		}
	}

	private final Map<Key<?>, U> values = new HashMap<>();

	public <T> void put(Key<T> key, U value) {
		values.put(key, value);
	}

	public <T> U get(Key<T> key) {
		return (U) values.get(key);
	}
}
