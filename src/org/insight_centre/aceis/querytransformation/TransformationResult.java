package org.insight_centre.aceis.querytransformation;

import java.util.List;

import org.insight_centre.aceis.eventmodel.EventDeclaration;

public class TransformationResult {
	private List<String> propertyList;
	private String queryStr;
	private List<String> serviceList, sensorList;
	private String qid = "";
	private EventDeclaration transformedFrom;

	public TransformationResult(List<String> serviceList, List<String> propertyList, String queryStr,
			EventDeclaration transformedFrom) {
		super();
		this.serviceList = serviceList;
		this.propertyList = propertyList;
		this.queryStr = queryStr;
		this.setTransformedFrom(transformedFrom);
	}

	public List<String> getPropertyList() {
		return propertyList;
	}

	public String getQueryStr() {
		return queryStr;
	}

	public List<String> getServiceList() {
		return serviceList;
	}

	public void setPropertyList(List<String> propertyList) {
		this.propertyList = propertyList;
	}

	public void setQueryStr(String queryStr) {
		this.queryStr = queryStr;
	}

	public void setServiceList(List<String> serviceList) {
		this.serviceList = serviceList;
	}

	public String getQid() {
		return qid;
	}

	public void setQid(String qid) {
		this.qid = qid;
	}

	public EventDeclaration getTransformedFrom() {
		return transformedFrom;
	}

	public void setTransformedFrom(EventDeclaration transformedFrom) {
		this.transformedFrom = transformedFrom;
	}

	public List<String> getSensorList() {
		return sensorList;
	}

	public void setSensorList(List<String> sensorList) {
		this.sensorList = sensorList;
	}
}
