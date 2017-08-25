package com.dataminer.util;

import java.util.HashMap;
import java.util.Map;


public class MultiTypeMap {

	public class Key<T> {
		final String identifier;
		final Class<T> type;

		public Key(String identifier, Class<T> type) {
			this.identifier = identifier;
			this.type = type;
		}
	}

//	private final Map<String, Key<?>> keys = new HashMap<>();
	private final Map<Key<?>, Object> values = new HashMap<>();

	public <T> void put(Key<T> key, T value) {
		values.put(key, value);
	}

//	public <T> T get(String keyName) {
//		Key<T> key = keys.get(keyName);
//		
//		keys.get(keyName).type.cast(values.get(keys.get(keyName)));
//	}
	
	
	public <T> T get(Key<T> key) {
		return key.type.cast(values.get(key));
	}
}


