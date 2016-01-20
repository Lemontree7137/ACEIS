package org.insight_centre.citypulse.commons.data.json;

import java.util.ArrayList;
import java.util.List;

import org.insight_centre.aceis.engine.ACEISEngine.RspEngine;
import org.insight_centre.aceis.eventmodel.QosVector;
import org.insight_centre.aceis.eventmodel.WeightVector;

import com.google.gson.Gson;

//import citypulse.commons.data.Coordinate;

public class DataFederationRequest {
	private QosVector constraint;
	private RspEngine engineType = RspEngine.CSPARQL;
	private boolean continuous;

	private List<Coordinate> route;

	private List<DataFederationPropertyType> propertyTypes;

	private WeightVector weight;

	public enum DataFederationPropertyType {
		air_quality, parking_availability, parking_cost, average_speed;
	}

	public DataFederationRequest(List<DataFederationPropertyType> propertyTypes, List<Coordinate> route,
			boolean continuous, QosVector constraint, WeightVector weight) {
		super();
		this.propertyTypes = propertyTypes;
		this.route = route;
		this.continuous = continuous;
		this.constraint = constraint;
		this.weight = weight;
	}

	public DataFederationRequest(RspEngine engineType, List<DataFederationPropertyType> propertyTypes,
			List<Coordinate> route, boolean continuous, QosVector constraint, WeightVector weight) {
		super();
		this.engineType = engineType;
		this.propertyTypes = propertyTypes;
		this.route = route;
		this.continuous = continuous;
		this.constraint = constraint;
		this.weight = weight;
	}

	public QosVector getConstraint() {
		return constraint;
	}

	public List<Coordinate> getCoordinates() {
		return route;
	}

	public List<DataFederationPropertyType> getPropertyTypes() {
		return propertyTypes;
	}

	public WeightVector getWeight() {
		return weight;
	}

	public boolean isContinuous() {
		return continuous;
	}

	public void setConstraint(QosVector constraint) {
		this.constraint = constraint;
	}

	public void setContinuous(boolean continuous) {
		this.continuous = continuous;
	}

	public void setCoordinates(List<Coordinate> route) {
		this.route = route;
	}

	public void setPropertyTypes(List<DataFederationPropertyType> propertyTypes) {
		this.propertyTypes = propertyTypes;
	}

	public void setWeight(WeightVector weight) {
		this.weight = weight;
	}

	public static void main(String[] args) {
		List<DataFederationPropertyType> ptypes = new ArrayList<DataFederationPropertyType>();
		ptypes.add(DataFederationPropertyType.air_quality);
		List<Coordinate> route = new ArrayList<Coordinate>();
		route.add(new Coordinate(56.01, 10.01));
		route.add(new Coordinate(56.02, 10.02));
		DataFederationRequest dfr = new DataFederationRequest(ptypes, route, false, null, null);
		Gson gson = new Gson();
		System.out.println(gson.toJson(dfr));
	}

	public RspEngine getEngineType() {
		return engineType;
	}

	public void setEngineType(RspEngine engineType) {
		this.engineType = engineType;
	}
}
