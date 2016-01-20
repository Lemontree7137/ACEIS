package org.insight_centre.aceis.eventmodel;

public class MsgBusGrounding {
	private String serverAddress;
	private String exchange;

	public String getServerAddress() {
		return serverAddress;
	}

	public void setServerAddress(String serverAddress) {
		this.serverAddress = serverAddress;
	}

	public String getExchange() {
		return exchange;
	}

	public void setExchange(String exchange) {
		this.exchange = exchange;
	}

	public String getTopic() {
		return topic;
	}

	public void setTopic(String topic) {
		this.topic = topic;
	}

	public String toString() {

		return this.serverAddress + ", " + this.exchange + ", " + this.topic;
	}

	private String topic;
}
