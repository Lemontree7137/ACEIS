package org.insight_centre.aceis.querytransformation;

import java.util.ArrayList;
import java.util.List;
import java.util.Stack;
import java.util.UUID;

import org.insight_centre.aceis.eventmodel.EventDeclaration;
import org.insight_centre.aceis.eventmodel.EventOperator;
import org.insight_centre.aceis.eventmodel.EventPattern;
import org.insight_centre.aceis.eventmodel.OperatorType;
import org.insight_centre.aceis.eventmodel.Selection;
import org.insight_centre.aceis.io.rdf.VirtuosoDataManager;

import com.hp.hpl.jena.rdf.model.Model;

public class CSPARQLQueryTransformer implements QueryTransformer {
	public static int window = 10;
	private String windowStr = "";
	// private String staticKBStr = "FROM <http://127.0.0.1:9000/WebGlCity/ces.rdf> ";
	private String staticKBStr = "  ";

	public String getStaticKBStr() {
		return staticKBStr;
	}

	public void setStaticKBStr(String staticKBStr) {
		this.staticKBStr = staticKBStr;
	}

	@Override
	public String transformFromRDF(Model m) {
		// TODO Auto-generated method stub
		return null;
	}

	private String getFromClause(EventDeclaration plan) throws CloneNotSupportedException {
		String result = "";
		if (plan.getEp().getTimeWindow() != 0)
			this.setTimeWindowStr("[RANGE " + plan.getEp().getTimeWindow() + "ms step " + plan.getEp().getTimeWindow()
					/ 2 + "ms]");
		for (EventDeclaration ed : plan.getEp().getEds()) {
			String timeWindowStr = "";
			if (ed.getEp() == null) {
				// int range = (int) (1500.0 / ed.getFrequency());
				// timeWindowStr = "[RANGE " + range + "ms step " + range / 3 + "ms]";
				timeWindowStr = "[RANGE 30000ms step 15000ms]";
			} else {
				// int maxRange = 0;
				// for (EventDeclaration ed2 : ed.getEp().clone().getEds()) {
				// int range = (int) (1000.0 / ed2.getFrequency());
				// if (maxRange <= range)
				// maxRange = range;
				// }
				// maxRange = (int) (1.5 * maxRange);
				// timeWindowStr = "[RANGE " + maxRange + "ms step " + maxRange / 3 + "ms]";
				timeWindowStr = "[RANGE 30000ms step 15000ms]";
			}
			result += " FROM STREAM <" + ed.getServiceId() + "> " + timeWindowStr + " \n";
		}
		result += staticKBStr;
		return result;

	}

	private String getBGP(Selection sel, List<String> vars, List<String> serviceList, List<String> propertyList,
			List<String> sensorList) {
		String observationIdVar = "?obId" + UUID.randomUUID().toString().replaceAll("-", "");
		String serviceIdVar = "<" + sel.getOriginalED().getServiceId() + ">";
		String propertyValueVar = "?value" + UUID.randomUUID().toString().replaceAll("-", "");
		String propertyTypeVar = "<" + sel.getPropertyType() + ">";
		String propertyIdVar = "?property" + UUID.randomUUID().toString().replaceAll("-", "");
		String propertyId = "<" + sel.getPropertyName() + ">";
		String sensorId = "<" + sel.getOriginalED().getSensorId() + ">";
		vars.add(observationIdVar);
		vars.add(propertyValueVar);
		serviceList.add(sel.getOriginalED().getServiceId());
		propertyList.add(sel.getPropertyType());
		sensorList.add(sel.getOriginalED().getSensorId());
		// TODO perhaps need to add property name into vars
		// vars.add(foiVar);
		// System.out.println("getting BGP: " + observationIdVar +
		// " rdf:type ?ob. " + observationIdVar
		// + " ssn:observedBy " + serviceIdVar + ". " + observationIdVar +
		// " ssn:observedProperty "
		// + propertyTypeVar + ". " + observationIdVar + " sao:hasValue " +
		// propertyValueVar + ". ");
		// return observationIdVar + " rdf:type ?ob. " + observationIdVar + " ssn:observedBy " + serviceIdVar + ". "
		// + observationIdVar + " ssn:observedProperty " + propertyIdVar + ". " + propertyIdVar + " rdf:type "
		// + propertyTypeVar + ". " + observationIdVar + " sao:hasValue " + propertyValueVar + ". ";
		return observationIdVar + " rdf:type ?ob. " + observationIdVar + " ssn:observedBy " + sensorId + ". "
				+ observationIdVar + " ssn:observedProperty " + propertyId + ". " + observationIdVar + " sao:value "
				+ propertyValueVar + ". ";
	}

	// private String getBoundFilters(List<String> nodeIds) {
	// return null;
	//
	// }

	private String getTimestampFilters(List<String> nodes) {
		// System.out.println(nodes);
		String result = "";
		for (int i = 0; i < nodes.size() - 1; i++) {
			result += this.parsePair(nodes.get(i), nodes.get(i + 1)) + " && ";
		}
		return "FILTER (" + result.substring(0, result.length() - 3) + ")";

	}

	private String parsePair(String s1, String s2) {
		if (isPrimitive(s1) && isPrimitive(s2)) {
			return "(f:timestamp" + s1 + ") < f:timestamp" + s2 + "))";
		} else {
			List<String> leftInnerFirsts = new ArrayList<String>();
			boolean leftAnd = this.findInnerFirstNodes(s1, leftInnerFirsts);
			List<String> rightInnerFirsts = new ArrayList<String>();
			boolean rightAnd = this.findInnerFirstNodes(s2, rightInnerFirsts);
			String result = "";
			for (String leftInnerFirst : leftInnerFirsts) {
				String lop = "&&";
				if (!leftAnd)
					lop = "||";
				String innerResult = "";
				for (String rightInnerFirst : rightInnerFirsts) {
					String rop = "&&";
					if (!rightAnd)
						rop = "||";
					innerResult += this.parsePair(leftInnerFirst, rightInnerFirst) + rop;
				}
				result += "(" + innerResult.substring(0, innerResult.length() - 2) + ")" + lop;
			}
			return "(" + result.substring(0, result.length() - 2) + ")";
		}
		// return null;
	}

	private boolean isPrimitive(String node) {
		if (node.contains("&") || node.contains("|"))
			return false;
		else
			return true;
	}

	private boolean findInnerFirstNodes(String str, List<String> results) {
		Stack leftParentheseIndex = new Stack<Integer>();
		boolean result = false;
		boolean resultDetermined = false;
		for (int i = 1; i < str.length() - 1; i++) {
			if (str.charAt(i) == '(')
				leftParentheseIndex.push(i);
			else if (str.charAt(i) == ')') {
				int index = (int) leftParentheseIndex.pop();
				if (leftParentheseIndex.isEmpty()) {
					results.add(str.substring(index, i + 1));
					if (!resultDetermined) {
						if (str.charAt(i + 1) == '&')
							result = true;
						resultDetermined = true;
					}
				}
			}
		}
		if (results.size() == 0) { // if no inner nodes found, it is a primitive
									// node, add itself to the list of resutls
			results.add(str);
		}
		return result;
	}

	@Override
	public TransformationResult transformFromED(EventDeclaration plan) throws Exception {
		String queryStr = VirtuosoDataManager.queryPrefix;
		String whereClause = "\n WHERE \n";
		String selClause = "\n SELECT DISTINCT \n";
		String fromClause = "\n " + this.getFromClause(plan);

		List<String> vars = new ArrayList<String>();
		List<String> serviceList = new ArrayList<String>();
		List<String> propertyList = new ArrayList<String>();
		List<String> sensorList = new ArrayList<String>();
		String whereSegment = this.getWhereSegment("", new ArrayList<String>(), new ArrayList<String>(), plan.getEp(),
				vars, serviceList, propertyList, sensorList);
		whereClause += whereSegment;
		for (String var : vars)
			selClause += (var + " ");
		queryStr = "REGISTER QUERY test AS " + queryStr + selClause + fromClause + whereClause;
		TransformationResult tr = new TransformationResult(serviceList, propertyList, queryStr, plan);
		tr.setSensorList(sensorList);
		if (plan.getComposedFor() != null)
			tr.setQid(plan.getComposedFor().getID());
		else
			tr.setQid("Query-" + UUID.randomUUID());
		return tr;
	}

	private String getWhereSegment(String queryStr, List<String> boundVar, List<String> timestampPatterns,
			EventPattern ep, List<String> vars, List<String> serviceList, List<String> propertyList,
			List<String> sensorList) throws Exception {
		Object root = ep.getNodeById(ep.getRootId());
		if (root instanceof EventDeclaration) {
			List<Selection> sels = ep.getSelectionOnNode(((EventDeclaration) root).getnodeId());
			boolean first = true;
			for (Selection sel : sels) {
				String bgp = this.getBGP(sel, vars, serviceList, propertyList, sensorList);
				queryStr += bgp;
				if (first) {
					boundVar.add(" bound(" + bgp.split(" ")[0] + ") ");
					timestampPatterns.add("(" + bgp.split(" ")[0] + ", rdf:type, ?ob)");
					// System.out.println("bound: " + boundVar + " , time: " +
					// timestampPatterns);
					first = false;
				}
			}
		} else {
			OperatorType opt = ((EventOperator) root).getOpt();
			if (opt.equals(OperatorType.rep))
				throw new Exception("Repetition not supported by C-SPARQL.");
			else if (opt.equals(OperatorType.and)) {
				List<EventPattern> subPatterns = ep.getDirectSubtrees();
				boolean first = true;
				String totalTimestampPattern = "";
				for (EventPattern subPattern : subPatterns) {
					// String subBoundFilters = "";
					List<String> tempBoundVar = new ArrayList<String>();
					List<String> tempTimestampPattern = new ArrayList<String>();
					queryStr += this.getWhereSegment("", tempBoundVar, tempTimestampPattern, subPattern, vars,
							serviceList, propertyList, sensorList);
					// System.out.println(tempBoundVar + " , " +
					// tempTimestampPattern);
					if (first && tempBoundVar.size() > 0) {
						boundVar.add(tempBoundVar.get(0));
						first = false;
					}
					if (tempTimestampPattern.size() > 0) {
						String t = tempTimestampPattern.get(0);
						totalTimestampPattern += t + "&";
					}
				}
				// if (!totalTimestampPattern.equals(""))
				timestampPatterns.add("(" + totalTimestampPattern.substring(0, totalTimestampPattern.length() - 1)
						+ ")");

				// queryStr += this.getBoundFilters(nodeIds);
			} else if (opt.equals(OperatorType.or)) {
				List<EventPattern> subPatterns = ep.getDirectSubtrees();
				String boundFilters = "";
				String totalTimestampPattern = "";
				for (EventPattern subPattern : subPatterns) {
					List<String> tempBoundVar = new ArrayList<String>();
					List<String> tempTimestampPatterns = new ArrayList<String>();
					queryStr += " OPTIONAL "
							+ this.getWhereSegment("", tempBoundVar, tempTimestampPatterns, subPattern, vars,
									serviceList, propertyList, sensorList);
					if (tempTimestampPatterns.size() > 0)
						totalTimestampPattern += tempTimestampPatterns.get(0) + "|";
					if (tempBoundVar.size() > 0)
						boundFilters += tempBoundVar.get(0) + "||";
				}
				timestampPatterns.add("(" + totalTimestampPattern.substring(0, totalTimestampPattern.length() - 1)
						+ ")");
				if (boundFilters.length() > 0)
					queryStr += " Filter (" + boundFilters.substring(0, boundFilters.length() - 2) + ")";
			} else if (opt.equals(OperatorType.seq)) {
				List<EventPattern> subPatterns = ep.getSortedDirectSubtrees();
				boolean first = true;
				List<String> subTimestampPatterns = new ArrayList<String>();
				for (EventPattern subPattern : subPatterns) {
					List<String> tempBoundVar = new ArrayList<String>();
					List<String> tempTimestampPatterns = new ArrayList<String>();
					queryStr += this.getWhereSegment("", tempBoundVar, tempTimestampPatterns, subPattern, vars,
							serviceList, propertyList, sensorList);
					if (first && tempBoundVar.size() > 0) {
						boundVar.add(tempBoundVar.get(0));
						first = false;
					}
					if (tempTimestampPatterns.equals(""))
						throw new Exception("A sub pattern without contain timestamp pattern detected.");
					subTimestampPatterns.add(tempTimestampPatterns.get(0));
				}
				timestampPatterns.add(subTimestampPatterns.get(subTimestampPatterns.size() - 1));
				String timestampFilters = this.getTimestampFilters(subTimestampPatterns);
				queryStr += timestampFilters;
			}
		}
		return "{" + queryStr + "}\n";
	}

	public static void main(String[] args) throws Exception {
		EventDeclaration plan = new EventDeclaration("queryPlan", "resultSrc", "complex", null, null, null);

		EventOperator and = new EventOperator(OperatorType.and, 1, "and");
		EventOperator seq = new EventOperator(OperatorType.seq, 1, "seq");
		EventOperator or = new EventOperator(OperatorType.or, 1, "or");
		EventDeclaration ed1 = new EventDeclaration("ed1", "srv1", "type1", null, null, null);
		ed1.setServiceId("srv1");
		EventDeclaration ed2 = new EventDeclaration("ed2", "srv2", "type2", null, null, null);
		ed2.setServiceId("srv2");
		EventDeclaration ed3 = new EventDeclaration("ed3", "srv3", "type3", null, null, null);
		ed3.setServiceId("srv3");
		EventDeclaration ed4 = new EventDeclaration("ed4", "srv4", "type4", null, null, null);
		ed4.setServiceId("srv4");
		// propertyName, providedBy, foi, propertyType
		List<Selection> sels = new ArrayList<Selection>();
		Selection sel1 = new Selection();
		sel1.setFoi("foi1");
		sel1.setOriginalED(ed1);
		sel1.setPropertyName("p1");
		sel1.setProvidedBy("ed1");
		sel1.setPropertyType("pt1");
		sels.add(sel1);
		Selection sel2 = new Selection();
		sel2.setFoi("foi2");
		sel2.setOriginalED(ed2);
		sel2.setPropertyName("p2");
		sel2.setProvidedBy("ed2");
		sel2.setPropertyType("pt2");
		sels.add(sel2);
		Selection sel3 = new Selection();
		sel3.setFoi("foi3");
		sel3.setOriginalED(ed3);
		sel3.setPropertyName("p3");
		sel3.setProvidedBy("ed3");
		sel3.setPropertyType("pt3");
		sels.add(sel3);
		Selection sel4 = new Selection();
		sel4.setFoi("foi4");
		sel4.setOriginalED(ed4);
		sel4.setPropertyName("p4");
		sel4.setProvidedBy("ed4");
		sel4.setPropertyType("pt4");
		sels.add(sel4);

		EventPattern queryPattern = new EventPattern();
		queryPattern.setSelections(sels);
		queryPattern.getEos().add(seq);
		queryPattern.getEos().add(or);
		queryPattern.getEos().add(and);
		queryPattern.getEds().add(ed1);
		queryPattern.getEds().add(ed2);
		queryPattern.getEds().add(ed3);
		queryPattern.getEds().add(ed4);

		queryPattern.getProvenanceMap().put("seq", new ArrayList<String>());
		queryPattern.getProvenanceMap().get("seq").add("and");
		queryPattern.getProvenanceMap().get("seq").add("or");
		queryPattern.getProvenanceMap().put("and", new ArrayList<String>());
		queryPattern.getProvenanceMap().put("or", new ArrayList<String>());
		queryPattern.getProvenanceMap().get("and").add("ed1");
		queryPattern.getProvenanceMap().get("and").add("ed2");
		queryPattern.getProvenanceMap().get("or").add("ed3");
		queryPattern.getProvenanceMap().get("or").add("ed4");

		queryPattern.getTemporalMap().put("and", "or");

		queryPattern.setTimeWindow(5);
		plan.setEp(queryPattern);

		CSPARQLQueryTransformer trans = new CSPARQLQueryTransformer();
		System.out.println(trans.transformFromED(plan).getQueryStr());
		System.out.println(trans.transformFromED(plan).getPropertyList());
		System.out.println(trans.transformFromED(plan).getServiceList());
	}

	public String getTimeWindowStr() {
		return windowStr;
	}

	public void setTimeWindowStr(String windowStr) {
		this.windowStr = windowStr;
	}
}
