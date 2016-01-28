package org.insight_centre.aceis.utils.test.qos;

import java.util.Date;

public class QualityUpdate implements Comparable<QualityUpdate> {
	private Double correctness;
	private Double latency;
	private Date timestamp;
	private String sensor_id;

	public String toString() {
		return "Sensor: " + sensor_id + ", correctness: " + correctness + ", latency: " + latency + ", @" + timestamp;

	}

	public String getSensor_id() {
		return sensor_id;
	}

	public void setSensor_id(String sensor_id) {
		this.sensor_id = sensor_id;
	}

	public QualityUpdate(Date timestamp, Double correctness, Double latency, String sid) {
		super();
		this.timestamp = timestamp;
		this.correctness = correctness;
		this.latency = latency;
		this.sensor_id = sid;
	}

	@Override
	public int compareTo(QualityUpdate other) {
		return this.timestamp.compareTo(other.timestamp);
	}

	public Double getCorrectness() {
		return correctness;
	}

	public Double getLatency() {
		return latency;
	}

	public Date getTimestamp() {
		return timestamp;
	}

	public void setCorrectness(Double correctness) {
		this.correctness = correctness;
	}

	public void setLatency(Double latency) {
		this.latency = latency;
	}

	public void setTimestamp(Date timestamp) {
		this.timestamp = timestamp;
	}

}
