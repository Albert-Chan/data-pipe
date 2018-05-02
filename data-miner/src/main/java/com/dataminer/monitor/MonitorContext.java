package com.dataminer.monitor;

import java.util.Map;

public class MonitorContext {
	private String appId;
	private String className;
	private Map<String, Object> params;

	private MonitorContext(String appId, String className, Map<String, Object> params) {
		this.appId = appId;
		this.className = className;
		this.params = params;
	}

	public static MonitorContext of(String appId, String className, Map<String, Object> params) {
		return new MonitorContext(appId, className, params);
	}

	public String toJSON() {
		return null;

	}

}
