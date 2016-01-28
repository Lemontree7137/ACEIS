package org.insight_centre.aceis.conflictresolution;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

import org.insight_centre.aceis.engine.ACEISEngine;
import org.insight_centre.aceis.engine.CompositionPlanEnumerator;
import org.insight_centre.aceis.engine.UtilityRanker;
import org.insight_centre.aceis.eventmodel.EventDeclaration;
import org.insight_centre.aceis.eventmodel.NodeRemovalException;
import org.insight_centre.aceis.eventmodel.QosVector;
import org.insight_centre.aceis.io.rdf.VirtuosoDataManager;
import org.insight_centre.aceis.utils.MapUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.hp.hpl.jena.rdf.model.Model;

public class SimpleSolver extends Solver {
	private enum solverMode {
		globalRanking, groupedRanking
	}

	public SimpleSolver(ACEISEngine engine) {
		super(engine);
	}

	private solverMode mode = solverMode.globalRanking;
	private static Logger logger = LoggerFactory.getLogger(SimpleSolver.class);

	@Override
	public String solve(List<String> streamIds) throws Exception {
		List<EventDeclaration> eds = new ArrayList<EventDeclaration>();
		for (String s : streamIds) {
			eds.add(this.getEngine().getRepo().getEds().get(s));
			// qoss.add(.getExternalQos());
		}
		if (this.mode == solverMode.globalRanking) {
			List<EventDeclaration> rankedEds = UtilityRanker.rankEdsWithAcc(eds);
			return rankedEds.get(0).getServiceId();
		} else {
			List<ArrayList<EventDeclaration>> groups = this.groupEDs(eds);
			Map<Integer, Double> accMap = new HashMap<Integer, Double>();
			int index = 0;
			for (ArrayList<EventDeclaration> group : groups) {
				double acc = 1.0;
				for (EventDeclaration ed : group)
					acc = acc * ed.getExternalQos().getAccuracy();
				accMap.put(index, acc);
				index += 1;
			}
			MapUtils.sortByValue(accMap);
			logger.info("sorted acc map: " + accMap);
			List<EventDeclaration> results = groups.get(accMap.keySet().iterator().next());

			for (String edid : streamIds)
				for (EventDeclaration ed : results)
					if (ed.getServiceId().equals(edid))
						return edid;
			return "";
		}

	}

	private List<ArrayList<EventDeclaration>> groupEDs(List<EventDeclaration> eds) throws Exception {
		Map<String, List<EventDeclaration>> valueEdMap = new HashMap<String, List<EventDeclaration>>();
		List<EventDeclaration> candidates = new ArrayList<EventDeclaration>();
		CompositionPlanEnumerator cpe = new CompositionPlanEnumerator(this.getEngine().getRepo(), null);
		for (EventDeclaration ed : eds) {
			List<EventDeclaration> partialCandidates = cpe.getAllSubsituteEDforED(ed);
			for (EventDeclaration candidate : partialCandidates)
				if (!candidates.contains(candidate))
					candidates.add(candidate);
		}

		for (EventDeclaration candidate : candidates) {
			HashMap<String, String> valueMap = VirtuosoDataManager.getSnapShot(candidate);
			MapUtils.sortByValue(valueMap);
			String key = "";
			for (Entry e : valueMap.entrySet()) {
				key += (e.getKey() + "-" + e.getValue() + ",");
			}
			if (valueEdMap.containsKey(key))
				valueEdMap.get(key).add(candidate);
			else {
				ArrayList<EventDeclaration> newCandidates = new ArrayList<EventDeclaration>();
				newCandidates.add(candidate);
				valueEdMap.put(key, newCandidates);
			}
		}
		List<ArrayList<EventDeclaration>> results = new ArrayList<ArrayList<EventDeclaration>>();
		for (List<EventDeclaration> edlist : valueEdMap.values())
			results.add((ArrayList<EventDeclaration>) edlist);
		return results;
	}

	public solverMode getMode() {
		return mode;
	}

	public void setMode(solverMode mode) {
		this.mode = mode;
	}
}
