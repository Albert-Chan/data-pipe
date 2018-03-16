package com.dataminer.monitor;

import java.util.HashMap;
import lombok.Data;
import lombok.ToString;

@ToString(includeFieldNames = true)
@Data(staticConstructor = "of")
public class MonitorContext {

	private String appId;
	private String appName;
	private String className;
	private HashMap<String, Object> params;

	public String toJSON() {
		return null;
				
	}

}
