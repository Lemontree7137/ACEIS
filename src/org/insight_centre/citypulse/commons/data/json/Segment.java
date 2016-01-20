package org.insight_centre.citypulse.commons.data.json;

public class Segment {
	private Location startLocation, endLocation;

	public Segment(Location startLocation, Location endLocation) {
		super();
		this.setStartLocation(startLocation);
		this.setEndLocation(endLocation);
	}

	public Location getStartLocation() {
		return startLocation;
	}

	public void setStartLocation(Location startLocation) {
		this.startLocation = startLocation;
	}

	public Location getEndLocation() {
		return endLocation;
	}

	public void setEndLocation(Location endLocation) {
		this.endLocation = endLocation;
	}
}
