package org.insight_centre.citypulse.commons.data.json;

public class SensorSnapshot {
	private String status, message, data, uuid;

	public String getStatus() {
		return status;
	}

	public void setStatus(String status) {
		this.status = status;
	}

	public String getMessage() {
		return message;
	}

	public void setMessage(String message) {
		this.message = message;
	}

	public String getData() {
		return data;
	}

	public void setData(String data) {
		this.data = data;
	}

	public String getUuid() {
		return uuid;
	}

	public void setUuid(String uuid) {
		this.uuid = uuid;
	}

	public SensorSnapshot(String status, String message, String data, String uuid) {
		super();
		this.status = status;
		this.message = message;
		this.data = data;
		this.uuid = uuid;
	}
}
