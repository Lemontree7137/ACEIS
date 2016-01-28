package org.insight_centre.citypulse.commons.data.json;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.insight_centre.aceis.eventmodel.QosVector;
import org.insight_centre.aceis.eventmodel.WeightVector;
//import org.insight_centre.contextualfilter.data.Path;
//import org.insight_centre.contextualfilter.data.Sensor;
//import org.insight_centre.contextualfilter.data.UserStatus;

import com.siemens.citypulse.resources.FunctionalDataset;
import com.siemens.citypulse.resources.FunctionalProperty;
import com.siemens.citypulse.resources.GlobalVariables.Modifier;
import com.siemens.citypulse.resources.GlobalVariables.Operator;
import com.siemens.citypulse.resources.Message;

public class JsonQuery {
	private String id;
	private List<Double> inflationFactors;
	// private Map<Sensor, Double> inflationMap = new HashMap<Sensor, Double>();
	private Message msg;
	private Path path;
	// private List<Sensor> sensors;
	// private UserStatus userStatus;
	private QosVector constraint;

	public QosVector getConstraint() {
		return constraint;
	}

	public void setConstraint(QosVector constraint) {
		this.constraint = constraint;
	}

	private WeightVector weight;

	public WeightVector getWeight() {
		return weight;
	}

	public void setWeight(WeightVector weight) {
		this.weight = weight;
	}

	public List<Double> getInflationFactors() {
		return inflationFactors;
	}

	// public Map<Sensor, Double> getInflationMap() {
	// return inflationMap;
	// }

	public Message getMsg() {
		return msg;
	}

	public Path getPath() {
		return path;
	}

	// public List<Sensor> getSensors() {
	// return sensors;
	// }
	//
	// public UserStatus getUserStatus() {
	// return userStatus;
	// }

	public void setInflationFactors(List<Double> inflationFactors) {
		this.inflationFactors = inflationFactors;
	}

	// public void setInflationMap(Map<Sensor, Double> inflationMap) {
	// this.inflationMap = inflationMap;
	// }

	public void setMsg(Message msg) {
		this.msg = msg;
	}

	public void setPath(Path path) {
		this.path = path;

	}

	// public void setSensors(List<Sensor> sensors) {
	// this.sensors = sensors;
	// }
	//
	// public void setUserStatus(UserStatus userStatus) {
	// this.userStatus = userStatus;
	// }

	public Map<String, Modifier> getModifierMap() {
		Map<String, Modifier> result = new HashMap<String, Modifier>();
		for (FunctionalDataset fd : this.getMsg().getFunctionalDatasets())
			for (FunctionalProperty fp : fd.getProperties()) {
				if (fp.getModifier() != null)
					result.put(fp.getName(), fp.getModifier());
			}
		return result;
	}

	public Map<String, String> getConstraintMap() {
		Map<String, String> result = new HashMap<String, String>();
		for (FunctionalDataset fd : this.getMsg().getFunctionalDatasets())
			for (FunctionalProperty fp : fd.getProperties()) {
				if (fp.getValue() != null)
					result.put(fp.getName(), fp.getValue());
			}
		// return null;
		return result;
	}

	public Map<String, Operator> getOperatorMap() {
		Map<String, Operator> result = new HashMap<String, Operator>();
		for (FunctionalDataset fd : this.getMsg().getFunctionalDatasets())
			for (FunctionalProperty fp : fd.getProperties()) {
				if (fp.getOperator() != null)
					result.put(fp.getName(), fp.getOperator());
			}
		return result;
	}

	public List<String> getProperties() {
		List<String> result = new ArrayList<String>();
		for (FunctionalDataset fd : this.getMsg().getFunctionalDatasets())
			for (FunctionalProperty fp : fd.getProperties()) {
				result.add(fp.getName());
			}
		return result;
	}

	public String getId() {
		return id;
	}

	public void setId(String id) {
		this.id = id;
	}
}
