package org.insight_centre.aceis.subscriptions;

import java.util.Date;

public class AdaptationResult {
	private String id;
	private boolean sucess;
	private String incrementalSuccessMode;

	public String getIncrementalSuccessMode() {
		return incrementalSuccessMode;
	}

	public void setIncrementalSuccessMode(String incrementalSuccessMode) {
		this.incrementalSuccessMode = incrementalSuccessMode;
	}

	public AdaptationResult(String id, boolean sucess, long time, Date timestamp) {
		super();
		this.id = id;
		this.sucess = sucess;
		this.time = time;
		this.timestamp = timestamp;
	}

	public String getId() {
		return id;
	}

	public void setId(String id) {
		this.id = id;
	}

	public boolean isSucess() {
		return sucess;
	}

	public void setSucess(boolean sucess) {
		this.sucess = sucess;
	}

	public long getTime() {
		return time;
	}

	public void setTime(long time) {
		this.time = time;
	}

	public Date getTimestamp() {
		return timestamp;
	}

	public void setTimestamp(Date timestamp) {
		this.timestamp = timestamp;
	}

	private long time;
	private Date timestamp;
}
