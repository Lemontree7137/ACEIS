package org.insight_centre.citypulse.commons.data.json;

import static java.lang.Math.asin;
import static java.lang.Math.cos;
import static java.lang.Math.sin;
import static java.lang.Math.sqrt;
import static java.lang.Math.toRadians;
//import org.insight_centre.contextualfilter.generator.UserPathGenerator;
//import com.graphhopper.routing.util.EncodingManager;
import com.siemens.citypulse.resources.Coordinate;

public class Location {
	int locationID;
	double lon, lat;
	/**
	 * mean radius of the earth
	 */
	public final static double R = 6371000; // m
	/**
	 * Radius of the earth at equator
	 */
	public final static double R_EQ = 6378137; // m

	public Location() {
		// TODO Auto-generated constructor stub
	}

	public Location(int id, double lat, double lon) {
		this.lat = lat;
		this.lon = lon;
		this.locationID = id;
	}

	public Location(double lat, double lon) {
		this.lat = lat;
		this.lon = lon;
		// this.locationID = UserPathGenerator.getIdFromCoordinate(lat, lon);

	}

	public Location(Coordinate c) {
		this.lat = c.getLat();
		this.lon = c.getLng();
		// this.locationID = UserPathGenerator.getIdFromCoordinate(lat, lon);

	}

	public int getLocationID() {
		return this.locationID;
	}

	public double getLat() {
		return this.lat;
	}

	public double getLon() {
		return this.lon;
	}

	public void setLocationID(int id) {
		this.locationID = id;
	}

	public void setLat(double lat) {
		this.lat = lat;
	}

	public void setLon(double lon) {
		this.lon = lon;
	}

	public double distance(Location l) {
		double toLat = l.getLat();
		double toLon = l.getLon();
		double fromLat = this.lat;
		double fromLon = this.lon;
		double sinDeltaLat = sin(toRadians(toLat - fromLat) / 2);
		double sinDeltaLon = sin(toRadians(toLon - fromLon) / 2);
		double normedDist = sinDeltaLat * sinDeltaLat + sinDeltaLon * sinDeltaLon * cos(toRadians(fromLat))
				* cos(toRadians(toLat));
		return Math.round(R_EQ * 2 * asin(sqrt(normedDist)));
	}

	public String toString() {
		return "Location(" + this.locationID + ", " + this.lat + ", " + this.lon + ")";
		// return "Location[ id = " + this.locationID + ", lat = " + this.lat + ", lon = " + this.lon + "]";
	}

}
