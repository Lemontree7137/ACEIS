package org.insight_centre.citypulse.commons.data.json;

public class Coordinate {
	/**
	 * mean radius of the earth
	 */
	public final static double R = 6371000; // m
	/**
	 * Radius of the earth at equator
	 */
	public final static double R_EQ = 6378137; // m

	double latitude, longitude;

	public Coordinate(final double latitude, final double longitude) {
		super();
		this.latitude = latitude;
		this.longitude = longitude;
	}

	public Coordinate(String stringCoordicate) throws Exception {

		String[] stringCoordicateTokens = stringCoordicate.split(" ");

		if (stringCoordicateTokens.length != 2) {
			throw new Exception("The string " + stringCoordicate + " does not have 2 parametes");
		} else {
			latitude = Double.parseDouble(stringCoordicateTokens[1]);
			longitude = Double.parseDouble(stringCoordicateTokens[0]);
		}

	}

	public double distance(final Coordinate l) {
		final double toLat = l.getLatitude();
		final double toLon = l.getLongitude();
		final double fromLat = latitude;
		final double fromLon = longitude;
		final double sinDeltaLat = Math.sin(Math.toRadians(toLat - fromLat) / 2);
		final double sinDeltaLon = Math.sin(Math.toRadians(toLon - fromLon) / 2);
		final double normedDist = sinDeltaLat * sinDeltaLat + sinDeltaLon * sinDeltaLon
				* Math.cos(Math.toRadians(fromLat)) * Math.cos(Math.toRadians(toLat));
		return Math.round(Coordinate.R_EQ * 2 * Math.asin(Math.sqrt(normedDist)));
	}

	public double getLatitude() {
		return latitude;
	}

	public String getLocationID() {
		return latitude + " " + longitude;
	}

	public double getLongitude() {
		return longitude;
	}

	public void setLatitude(final double latitude) {
		this.latitude = latitude;
	}

	public void setLongitude(final double longitude) {
		this.longitude = longitude;
	}

	@Override
	public String toString() {
		return latitude + " " + longitude;
	}

	/*
	 * @author: Thu-Le Pham
	 */
	public String createCoordinateId() {
		StringBuilder asp = new StringBuilder();
		asp.append("\"").append(this.latitude).append(this.longitude).append("\"");
		return asp.toString();
	}

}
