package org.insight_centre.aceis.querytransformation;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;

import org.insight_centre.aceis.eventmodel.EventDeclaration;
import org.insight_centre.aceis.eventmodel.Filter;
import org.insight_centre.aceis.eventmodel.Selection;
import org.insight_centre.aceis.io.rdf.VirtuosoDataManager;

import com.hp.hpl.jena.rdf.model.Model;

public class CQELSQueryTransformer implements QueryTransformer {
	private String timeWindowStr;

	// public static int window = 10;

	public String transformFromRDF(Model eventRequest) {

		return null;
	}

	public TransformationResult transformFromED(EventDeclaration plan) throws Exception {
		if (plan.getEp() == null)
			throw new Exception("Nil composition plan.");
		String queryStr = VirtuosoDataManager.queryPrefix;
		// List<String> edSrcs = new ArrayList<String>();
		String whereClause = " where { \n";
		String selClause = "\n Select  ";
		List<String> vars = new ArrayList<String>();
		List<String> serviceList = new ArrayList<String>();
		List<String> sensorList = new ArrayList<String>();
		List<String> propertyList = new ArrayList<String>();
		for (EventDeclaration ed : plan.getEp().getEds()) {
			// String timewindow = " [range " + plan.getEp().getTimeWindow() + "s] ";
			String timeWindowStr = "";
			if (ed.getEp() == null) {
				int range = (int) (1500.0 / ed.getFrequency());
				timeWindowStr = "[RANGE " + range + "ms ]";
			} else {
				int maxRange = 0;
				for (EventDeclaration ed2 : ed.getEp().clone().getEds()) {
					int range = (int) (1000.0 / ed2.getFrequency());
					if (maxRange <= range)
						maxRange = range;
				}
				maxRange = (int) (1.5 * maxRange);
				timeWindowStr = "[RANGE " + maxRange + "ms ]";
			}
			timeWindowStr = "[RANGE 30000ms ]";
			String whereSegment = " stream <" + ed.getServiceId() + ">" + timeWindowStr + " { ";
			List<Selection> sels = plan.getEp().getSelectionOnNode(ed.getnodeId());
			Map<String, List<Filter>> filterMap = plan.getEp().getFilters();
			if (sels.size() > 0)
				for (Selection sel : sels) {
					// String propertyType = sel.getPropertyType();
					// String propertyName = sel.getPropertyName();
					String observationIdVar = "?obId" + UUID.randomUUID().toString().replaceAll("-", "");
					// String serviceIdVar = "?service" + UUID.randomUUID().toString().replaceAll("-", "");//
					// sel.getOriginalED().getServiceId();
					String serviceIdVar = "<" + sel.getOriginalED().getServiceId() + ">";
					String sensorIdVar = "<" + sel.getOriginalED().getSensorId() + ">";
					// if (cnt == 0)
					// serviceIdVar = "?srv" + UUID.randomUUID().toString().replaceAll("-", "");
					String propertyValueVar = "?value" + UUID.randomUUID().toString().replaceAll("-", "");
					// String propertyTypeVar = "?type" + UUID.randomUUID().toString().replaceAll("-", "");
					String propertyTypeVar = "<" + sel.getPropertyType() + ">";
					String propertyIdVar = "?property" + UUID.randomUUID().toString().replaceAll("-", "");//
					// sel.getPropertyName();
					// String foiVar = "?foi" + UUID.randomUUID().toString().replaceAll("-", "");// sel.getFoi();
					vars.add(observationIdVar);
					// vars.add(propertyTypeVar);
					vars.add(propertyValueVar);
					serviceList.add(sel.getOriginalED().getServiceId());
					propertyList.add(sel.getPropertyType());
					sensorList.add(sel.getOriginalED().getSensorId());
					String propertyId = "<" + sel.getPropertyName() + ">";
					// TODO perhaps need to add property name into vars
					// vars.add(foiVar);

					// whereSegment += observationIdVar + " rdf:type ?ob. " + observationIdVar + " ssn:observedBy "
					// + sensorIdVar + ". " + observationIdVar + " ssn:observedProperty " + propertyIdVar + ". "
					// + propertyIdVar + " rdf:type " + propertyTypeVar + ". " + observationIdVar
					// + " sao:hasValue " + propertyValueVar + ". ";

					whereSegment += observationIdVar + " rdf:type ?ob. " + observationIdVar + " ssn:observedBy "
							+ sensorIdVar + ". " + observationIdVar + " ssn:observedProperty " + propertyId + ". "
							+ observationIdVar + " sao:value " + propertyValueVar + ". ";

					// whereSegment += observationIdVar + " rdf:type ?ob. " + observationIdVar + " ssn:observedBy "
					// + serviceIdVar + ". " + observationIdVar + " sao:hasValue " + propertyValueVar + ". ";
				}
			else {// TODO no selections specified, by default all properties are selected.

			}
			whereSegment += " } \n";
			whereClause += whereSegment;
		}
		for (String var : vars)
			selClause += (var + " ");
		Map<String, List<String>> results = new HashMap<String, List<String>>();
		queryStr = queryStr + " " + selClause + "\n " + whereClause + " }";
		// results.put(, serviceList);
		TransformationResult tr = new TransformationResult(serviceList, propertyList, queryStr, plan);
		tr.setSensorList(sensorList);
		// if (cnt == 0)
		// selClause = "\n Select * ";
		// String s =
		// "Select ?loc ?sumETA From <KB> Where {?seg1Evt a sao:StreamData.?seg2Evt a sao:StreamData.?seg3Evt a sao:StreamData.?locEvt a sao:StreamData. Stream<LocationStreamUrl> {?locId a ?locEvt. ?locId sao:value ?loc. } Stream<Seg1CongestionStreamUrl> {?seg1Id a ?seg1Evt. ?seg1Id sao:value ?eta1. } Stream<Seg2CongestionStreamUrl> {?seg2Id a ?seg2Evt. ?seg2Id sao:value ?eta2. } Stream<Seg3CongestionStreamUrl> {?seg3Id a ?seg3Evt. ?seg3Id sao:value ?eta3. } Bind ((?eta1+?eta2+?eta3) as ?sumETA)}";
		return tr;
	}

	public String loadQueryFromFile(String filePath) throws IOException {
		BufferedReader br = new BufferedReader(new FileReader(filePath));
		String queryStr = "";
		String line;
		while ((line = br.readLine()) != null) {
			queryStr += line;
		}
		br.close();
		return queryStr;
	}

	public List<String> getServiceList(String queryStr) {
		List<String> results = new ArrayList<String>();
		return results;
	}

	@Override
	public String getTimeWindowStr() {
		return this.timeWindowStr;
	}

	@Override
	public void setTimeWindowStr(String str) {
		this.timeWindowStr = str;

	}
}
