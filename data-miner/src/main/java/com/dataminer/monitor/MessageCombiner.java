package com.dataminer.monitor;

public class MessageCombiner {

	private StringBuilder keyBuilder = new StringBuilder();
	private long when;
	private String eventType;
	private String event;

	public MessageCombiner partOfKey(String name, String value) {
		if (keyBuilder.length() != 0) {
			keyBuilder.append(";");
		}
		keyBuilder.append(name).append("=").append(value);
		return this;
	}

	/** specify the event time */
	public MessageCombiner when(long timestamp) {
		this.when = timestamp;
		return this;
	}

	public Message event(String eventType, String event) {
		this.eventType = eventType;
		this.event = event;
		return new Message(getKey(), getMessage());
	}

	private String getKey() {
		return keyBuilder.toString();
	}

	private String getMessage() {
		return String.format("%s;time=%d;%s=%s", getKey(), when == 0L ? System.currentTimeMillis() : when, eventType,
				event);
	}

}
