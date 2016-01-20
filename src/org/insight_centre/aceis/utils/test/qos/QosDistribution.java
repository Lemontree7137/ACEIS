package org.insight_centre.aceis.utils.test.qos;

import java.text.SimpleDateFormat;
import java.util.TreeMap;

public class QosDistribution {
	private TreeMap<Double, Long> accDistMap, latDistMap, completenessDistMap, secDistMap, priceDistMap,
			trafficDistMap;
	private String sensorId;
	private SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss");
	private long cnt = 0;

	public long getCnt() {
		return cnt;
	}

	public void setCnt(long cnt) {
		this.cnt = cnt;
	}

	public QosDistribution(String sensorId) {
		super();
		this.sensorId = sensorId;
		this.accDistMap = new TreeMap<Double, Long>();
		this.latDistMap = new TreeMap<Double, Long>();
		this.completenessDistMap = new TreeMap<Double, Long>();
		this.secDistMap = new TreeMap<Double, Long>();
		this.priceDistMap = new TreeMap<Double, Long>();
		this.trafficDistMap = new TreeMap<Double, Long>();
	}

	public TreeMap<Double, Long> getAccDistMap() {
		return accDistMap;
	}

	public TreeMap<Double, Long> getCompletenessDistMap() {
		return completenessDistMap;
	}

	public TreeMap<Double, Long> getLatDistMap() {
		return latDistMap;
	}

	public TreeMap<Double, Long> getPriceDistMap() {
		return priceDistMap;
	}

	public TreeMap<Double, Long> getSecDistMap() {
		return secDistMap;
	}

	public TreeMap<Double, Long> getTrafficDistMap() {
		return trafficDistMap;
	}

	public void setAccDistMap(TreeMap<Double, Long> accDistMap) {
		this.accDistMap = accDistMap;
	}

	public void setCompletenessDistMap(TreeMap<Double, Long> completenessDistMap) {
		this.completenessDistMap = completenessDistMap;
	}

	public void setLatDistMap(TreeMap<Double, Long> latDistMap) {
		this.latDistMap = latDistMap;
	}

	public void setPriceDistMap(TreeMap<Double, Long> priceDistMap) {
		this.priceDistMap = priceDistMap;
	}

	public void setSecDistMap(TreeMap<Double, Long> secDistMap) {
		this.secDistMap = secDistMap;
	}

	public void setTrafficDistMap(TreeMap<Double, Long> trafficDistMap) {
		this.trafficDistMap = trafficDistMap;
	}
}