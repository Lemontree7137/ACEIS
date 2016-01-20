package org.insight_centre.aceis.engine;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

import org.insight_centre.aceis.eventmodel.EventDeclaration;
import org.insight_centre.aceis.eventmodel.EventPattern;
import org.insight_centre.aceis.eventmodel.QosVector;
import org.insight_centre.aceis.eventmodel.WeightVector;
import org.insight_centre.aceis.utils.MapUtils;

/**
 * @author feng
 * 
 *         Utility ranker
 */
public class UtilityRanker {
	public static List<EventPattern> rank(List<EventPattern> eps, QosVector constraint, WeightVector weight)
			throws Exception {
		Map<EventPattern, Double> utilityMap = new HashMap<EventPattern, Double>();
		for (EventPattern ep : eps)
			utilityMap.put(ep, Comparator.calculateUtility(constraint, weight, ep.aggregateQos()));
		MapUtils.sortByValue(utilityMap);
		List<EventPattern> results = new ArrayList<EventPattern>();
		for (Entry<EventPattern, Double> e : utilityMap.entrySet())
			results.add(e.getKey());
		return results;

	}

	public static List<EventDeclaration> rankEds(List<EventDeclaration> eds, QosVector constraint, WeightVector weight)
			throws Exception {
		Map<EventDeclaration, Double> utilityMap = new HashMap<EventDeclaration, Double>();
		for (EventDeclaration ed : eds)
			utilityMap.put(ed, Comparator.calculateUtility(constraint, weight, ed.getExternalQos()));
		MapUtils.sortByValue(utilityMap);
		List<EventDeclaration> results = new ArrayList<EventDeclaration>();
		for (Entry<EventDeclaration, Double> e : utilityMap.entrySet())
			results.add(e.getKey());
		return results;

	}
}
