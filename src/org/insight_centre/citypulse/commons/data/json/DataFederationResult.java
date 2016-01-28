package org.insight_centre.citypulse.commons.data.json;

import java.util.ArrayList;
import java.util.HashMap;

//import citypulse.commons.event_request.DataFederationRequest.DataFederationPropertyType;

public class DataFederationResult {
	private HashMap<String, ArrayList<String>> result;

	public DataFederationResult() {
		this.result = new HashMap<String, ArrayList<String>>();
		// this.result.put(PropertyType.air_quality, new ArrayList<String>());
	}

	public DataFederationResult(HashMap<String, ArrayList<String>> result) {
		super();
		this.result = result;
	}

	public HashMap<String, ArrayList<String>> getResult() {
		return result;
	}

	public void setResult(HashMap<String, ArrayList<String>> result) {
		this.result = result;
	}
}
