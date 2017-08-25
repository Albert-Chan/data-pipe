package com.dataminer.schema;

import java.util.List;

public class Schema {

	List<SchemaItem> items;

	Class reflect() {
		//return Class.class;
	}

}

class SchemaItem {
	String name;
	String valueType;
}