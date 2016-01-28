package org.insight_centre.aceis.engine;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.UUID;

import org.insight_centre.aceis.eventmodel.EventDeclaration;
import org.insight_centre.aceis.eventmodel.EventOperator;
import org.insight_centre.aceis.eventmodel.EventPattern;
import org.insight_centre.aceis.eventmodel.NodeRemovalException;
import org.insight_centre.aceis.eventmodel.QosVector;
import org.insight_centre.aceis.eventmodel.Selection;
import org.insight_centre.aceis.eventmodel.WeightVector;
import org.insight_centre.aceis.io.EventRepository;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * @author feng
 * 
 *         Qos-aware event service composition module, reuses some functionalities from Composer.java. Can be used to
 *         carry out brute-force enumeration of composition plans, also provides functionalities used in Genetic
 *         Evolutions
 */
public class CompositionPlanEnumerator {
	public static final Logger logger = LoggerFactory.getLogger(CompositionPlanEnumerator.class);

	private EventPattern query;
	private EventRepository repo;

	// private Session session;

	// public Session getSession() {
	// return session;
	// }
	//
	// public void setSession(Session session) {
	// this.session = session;
	// }

	public CompositionPlanEnumerator(EventRepository repo, EventPattern query) {
		super();
		this.repo = repo;
		this.setQuery(query);
	}

	// private Composer composer;

	public EventPattern calculateBestFitness(WeightVector weight, QosVector constraint) throws Exception {
		// logger.info("Calculating best QoS utility with Brute-Force enumeration.");
		long t1 = System.currentTimeMillis();
		List<EventPattern> ccps = this.getCCPsForACPs(this.getACPs());
		logger.info("Calculating best QoS utility with Brute-Force enumeration.");
		logger.info("Total ccps: " + ccps.size());
		Double bestFitness = -30.0;
		Double worstFitness = 30.0;
		Double sum = 0.0;
		// Double minTrafficDemand = 100.0;
		// Double avgTrafficDemand = 0.0;
		// Double maxTrafficDemand = 0.0;
		// Double sumTraffic = 0.0;
		// EventPattern maxTrafficEp = new EventPattern();
		// EventPattern minTrafficEp = new EventPattern();
		EventPattern best = new EventPattern();
		EventPattern worst = new EventPattern();
		// Map<Double, Integer> resultMap = new HashMap<Double, Integer>();
		// Map<Integer, Integer> resultMap2 = new HashMap<Integer, Integer>();
		// resultMap.put(0.3, 0);
		// resultMap.put(0.6, 0);
		// resultMap.put(0.9, 0);
		// resultMap.put(3.0, 0);
		for (EventPattern ccp : ccps) {
			QosVector qos = ccp.aggregateQos();
			Double ccpFitness = null;
			try {
				if (qos.satisfyConstraint(constraint))
					ccpFitness = Comparator.calculateUtility(constraint, weight, qos);
			} catch (Exception e) {
				e.printStackTrace();
				// if (this.session != null){
				// session.getBasicRemote().sendText(e.getMessage());
				// }
				continue;
				// throw e;
			}
			if (ccpFitness == null)
				continue;
			sum += ccpFitness;
			// sumTraffic += qos.getTraffic();
			// if (ccpFitness <= 0.3)
			// resultMap.put(0.3, resultMap.get(0.3) + 1);
			// else if (ccpFitness <= 0.6)
			// resultMap.put(0.6, resultMap.get(0.6) + 1);
			// else if (ccpFitness <= 0.9)
			// resultMap.put(0.9, resultMap.get(0.9) + 1);
			// else
			// resultMap.put(3.0, resultMap.get(3.0) + 1);

			int epSize = ccp.getSize();
			// if (resultMap2.get(epSize) != null)
			// resultMap2.put(epSize, resultMap2.get(epSize) + 1);
			// else
			// resultMap2.put(epSize, 0);

			if (ccpFitness > bestFitness) {
				bestFitness = ccpFitness;
				best = ccp;
			}
			if (ccpFitness < worstFitness) {
				worstFitness = ccpFitness;
				worst = ccp;
			}
			// if (qos.getTraffic() > maxTrafficDemand) {
			// maxTrafficDemand = qos.getTraffic();
			// maxTrafficEp = ccp;
			// }
			// if (qos.getTraffic() < minTrafficDemand) {
			// minTrafficDemand = qos.getTraffic();
			// minTrafficEp = ccp;
			// }

		}

		// System.out.println(resultMap);
		// System.out.println(resultMap2);
		// long t2 = System.currentTimeMillis();
		// logger.info("Avg : " + sum / ccps.size() + " Best: " + bestFitness + " Worst: " + worstFitness);
		// logger.info("Best ccp: " + best.toSimpleString());
		// logger.info("Worst ccp: " + worst.toSimpleString());
		// logger.info("Best QoS calculated in: " + (t2 - t1) + " ms.");
		// System.out.println("Avg Traffic: " + sumTraffic / ccps.size() + " max: " + maxTrafficDemand + " min: "
		// + minTrafficDemand);
		// System.out.println("max Traffic ep: " + maxTrafficEp);
		// System.out.println("min Traffic ep: " + minTrafficEp);
		return best;
	}

	private List<List<String>> cartesianProduct(List<List<String>> lists) {
		List<List<String>> resultLists = new ArrayList<List<String>>();
		if (lists.size() == 0) {
			resultLists.add(new ArrayList<String>());
			return resultLists;
		} else {
			List<String> firstList = lists.get(0);
			List<List<String>> remainingLists = cartesianProduct(lists.subList(1, lists.size()));
			for (String condition : firstList) {
				for (List<String> remainingList : remainingLists) {
					ArrayList<String> resultList = new ArrayList<String>();
					resultList.add(condition);
					resultList.addAll(remainingList);
					resultLists.add(resultList);
				}
			}
		}
		return resultLists;
	}

	private EventPattern createEPwithReplacements(EventPattern original, List<String> reusableNodes,
			List<String> replacements, Map<String, EventPattern> parts) throws Exception {
		EventPattern result = original.clone();
		for (int i = 0; i < replacements.size(); i++) {
			result.replaceSubtree(reusableNodes.get(i), parts.get(replacements.get(i)));
		}
		// if no replacements, the pattern itself is returned
		result.setID("ACP-" + UUID.randomUUID());
		parts.put(result.getID(), result);
		return result;
	}

	/**
	 * @return
	 * @throws Exception
	 * 
	 *             creates abstract composition plans (ACP) for the given query, an ACP is a composition plan that do
	 *             not care about the actual event service bindings for the leaf nodes in the pattern.
	 */
	public List<EventPattern> getACPs() throws Exception {
		// String rootId = query.getRootId();
		long t1 = System.currentTimeMillis();
		Map<String, List<String>> reusabilities = this.getReusableNodes(query.getID());
		logger.info("Getting Abstract Composition Plans for query: " + query.toSimpleString());
		logger.debug("reusable nodes: " + reusabilities);
		Map<String, EventPattern> compositionParts = new HashMap<String, EventPattern>();
		List<EventPattern> results = this.implementACP(query, reusabilities, compositionParts);
		long t2 = System.currentTimeMillis();
		logger.info("Abstract Composition Plans created in: " + (t2 - t1) + " ms with " + results.size() + " ACPs.");
		for (EventPattern acp : results)
			logger.info("ACP: " + acp.toSimpleString());
		return results;
	}

	public List<EventPattern> getACPsForQuery(EventPattern query) throws Exception {
		// String rootId = query.getRootId();
		long t1 = System.currentTimeMillis();
		logger.debug("Getting Abstract Composition Plans for: " + query.toSimpleString());
		Map<String, List<String>> reusabilities = this.getReusableNodes(query.getID());
		logger.debug("reusable nodes: " + reusabilities);
		Map<String, EventPattern> compositionParts = new HashMap<String, EventPattern>();
		// for(EventPattern acp:)
		List<EventPattern> results = this.implementACP(query, reusabilities, compositionParts);
		// for (EventPattern acp : results)
		// System.out.println("ACP: " + acp);
		long t2 = System.currentTimeMillis();
		logger.debug("Abstract Composition Plans created in: " + (t2 - t1) + " ms with " + results.size() + " ACPs.");
		return results;
	}

	public List<EventDeclaration> getAllSubsituteEDforED(EventDeclaration ed) throws CloneNotSupportedException,
			NodeRemovalException {
		logger.debug("Getting substitutes for: " + ed);
		List<EventDeclaration> results = new ArrayList<EventDeclaration>();
		if (ed.getEp() == null) {
			for (Entry e : this.repo.getEds().entrySet()) {
				if (((EventDeclaration) e.getValue()).getEp() != null)
					continue;
				// if (((EventDeclaration) e.getValue()).getEventType().equals(ed.getEventType())
				// && ((EventDeclaration) e.getValue()).getFoi().equals(ed.getFoi()))

				if (Comparator.isSubstitutePrimitiveEvent(ed, (EventDeclaration) e.getValue()))
					results.add((EventDeclaration) e.getValue());
			}
		} else {
			for (Entry e : this.repo.getEds().entrySet()) {
				EventDeclaration candidate = ((EventDeclaration) e.getValue());
				if (candidate.getEp() == null)
					continue;
				if (Comparator.isSubstitute(candidate.getEp(), ed.getEp())) {
					results.add(candidate);
				}
			}

		}
		// logger.debug("Found subs: " + results);
		return results;
	}

	public EventDeclaration getBestSubsitituteEDforED(EventDeclaration ed, QosVector constraint, WeightVector weight)
			throws Exception {
		// TODO add get best substitute function
		List<EventDeclaration> candidates = this.getAllSubsituteEDforED(ed);
		UtilityRanker.rankEds(candidates, constraint, weight);
		// if (candidates.get(candidates.size() - 1).getExternalQos().satisfyConstraint(constraint))
		logger.info("Best substitute: " + candidates.get(candidates.size() - 1) + ", for: " + ed);
		return candidates.get(candidates.size() - 1);
		// return null;
	}

	/**
	 * @param acp
	 * @return
	 * @throws Exception
	 * 
	 *             creates all possible concrete composition plans (CCPs) for a given ACP. A CCP is concrete in the
	 *             sense that it has complete event service binding information for all of its leaf nodes.
	 */
	public List<EventPattern> getCCPsforACP(EventPattern acp) throws Exception {
		System.out.println("GETTING CCPs FOR: " + acp);
		List<List<String>> replacementLists = new ArrayList<List<String>>();
		List<EventPattern> results = new ArrayList<EventPattern>();
		for (EventDeclaration ed : acp.getEds()) {
			List<String> replacementList = new ArrayList<String>();
			// if (this.repo.getEds().containsKey(ed.getID()))
			// replacementList.add(ed.getID());
			List<EventDeclaration> replacements = this.getAllSubsituteEDforED(ed);
			for (EventDeclaration replacement : replacements)
				replacementList.add(replacement.getnodeId());
			replacementLists.add(replacementList);
		}
		System.out.println("Replacement Lists: " + replacementLists);
		List<List<String>> combinations = this.cartesianProduct(replacementLists);
		for (List<String> combination : combinations) {
			EventPattern ccp = acp.clone();
			for (int i = 0; i < combination.size(); i++) {
				EventDeclaration replacementED = this.repo.getEds().get(combination.get(i));
				ccp.replaceED(acp.getEds().get(i).getnodeId(), replacementED);
				ccp.setID("CCP-" + acp.getID() + "-" + results.size());

				// if (ccp.getSize() == 1)

			}
			ccp.setQuery(false);
			// System.out.println("ADDING CCP: " + ccp);
			results.add(ccp);
		}

		return results;
	}

	/**
	 * @param acps
	 * @return
	 * @throws Exception
	 * 
	 *             creates concrete composition plans (CCPs) for a list of ACPs.
	 */
	public List<EventPattern> getCCPsForACPs(List<EventPattern> acps) throws Exception {
		long t1 = System.currentTimeMillis();
		List<EventPattern> results = new ArrayList<EventPattern>();
		for (EventPattern acp : acps) {
			results.addAll(this.getCCPsforACP(acp));
		}
		long t2 = System.currentTimeMillis();
		logger.debug("Brute-Force enmueration of Complete Composition Plans completed in: " + (t2 - t1) + " ms with "
				+ results.size() + " CCPs.");
		return results;
	}

	public List<EventPattern> getCCPsForQuery(EventPattern query) throws Exception {
		List<EventPattern> acps = this.getACPsForQuery(query);
		List<EventPattern> results = new ArrayList<EventPattern>();
		for (EventPattern acp : acps) {
			List<EventPattern> ccps = this.getCCPsforACP(acp);
			results.addAll(ccps);
		}
		return results;
	}

	/**
	 * @param ep
	 * @param reusabilities
	 * @return get valid combinations of in-direct reusable event patterns on the root of ep
	 * @throws NodeRemovalException
	 * @throws CloneNotSupportedException
	 */
	private List<List<String>> getIRcombinations(EventPattern ep, Map<String, List<String>> reusabilities)
			throws CloneNotSupportedException, NodeRemovalException {
		String rootId = ep.getRootId();
		List<String> irReplacements = new ArrayList<String>();
		List<List<String>> results = new ArrayList<List<String>>();
		for (Entry<String, List<String>> entry : reusabilities.entrySet()) {
			if (entry.getKey().equals(rootId)) {
				for (String reusable : entry.getValue()) {
					if (reusable.contains("ir|"))
						irReplacements.add(reusable.split("\\|")[1]);
				}
			}
		}
		if (ep.getSize() == 6)
			logger.debug("Ir replacements " + irReplacements.size() + ": ");
		List<String> combCodes = new ArrayList<String>();
		for (int i = 1; i < Math.pow(2, irReplacements.size()); i++) {
			// logger.info("It: " + i);
			String binaryStr = Integer.toBinaryString(i);
			while (binaryStr.length() < irReplacements.size())
				binaryStr = "0" + binaryStr;
			combCodes.add(binaryStr);
		}
		logger.debug("comb codes: " + combCodes);
		for (String code : combCodes) {
			List<String> replacementComb = new ArrayList<String>();
			for (int i = 0; i < code.length(); i++) {
				if (Integer.parseInt(code.charAt(i) + "") == 1)
					replacementComb.add(irReplacements.get(i));
			}
			if (!hasOverlap(replacementComb))
				results.add(replacementComb);
		}
		return results;
	}

	public EventPattern getQuery() {
		return query;
	}

	public EventPattern getRandomCCPforACP(EventPattern eventPattern, List<EventDeclaration> candidates)
			throws Exception {
		EventPattern ccp = eventPattern.clone();
		Map<String, EventDeclaration> replacementMap = new HashMap<String, EventDeclaration>();
		for (EventDeclaration ed : ccp.getEds()) {
			try {
				EventDeclaration replacement = getRandomSubstituteEDforED(ed, candidates);
				// ccp.replaceED(ed.getID(), replacement);
				replacementMap.put(ed.getnodeId(), replacement);
			} catch (Exception e) {
				logger.error("failed finding replacement for: " + ed.getServiceId());
				System.exit(0);
			}
		}
		for (Entry<String, EventDeclaration> e : replacementMap.entrySet())
			if (e.getValue() != null)
				ccp.replaceED(e.getKey(), e.getValue());
			else
				logger.debug("1 of the replacements is null.");
		ccp.setQuery(false);
		return ccp;
	}

	// public static EventPattern getRandomEDforACP(EventPattern acp, List<EventDeclaration> candidates) throws
	// Exception {
	// EventPattern ccp = acp.clone();
	// Map<String, EventDeclaration> replacementMap = new HashMap<String, EventDeclaration>();
	// for (EventDeclaration ed : ccp.getEds()) {
	// EventDeclaration replacement = getRandomSubstituteEDforED(ed, candidates);
	// // ccp.replaceED(ed.getID(), replacement);
	// replacementMap.put(ed.getnodeId(), replacement);
	// }
	// // System.out.println("----------");
	// // System.out.println("replacements: " + replacementMap);
	// // System.out.println("replacements before: " + ccp);
	// for (Entry<String, EventDeclaration> e : replacementMap.entrySet())
	// ccp.replaceED(e.getKey(), e.getValue());
	// // System.out.println("replacement result: " + ccp);
	// // System.out.println("----------");
	// return ccp;
	// }

	public EventDeclaration getRandomSubstituteEDforED(EventDeclaration ed, List<EventDeclaration> candidates)
			throws CloneNotSupportedException, NodeRemovalException {
		List<EventDeclaration> substitutes = new ArrayList<EventDeclaration>();
		if (ed.getEp() == null) {
			for (EventDeclaration e : candidates) {
				if (e.getEp() != null || e.getServiceId().equals(ed.getServiceId()))
					continue;
				if (Comparator.isSubstitutePrimitiveEvent(e, ed))
					substitutes.add(e);
			}
		} else {
			for (EventDeclaration e : candidates) {
				// EventDeclaration candidate = ((EventDeclaration) e.getValue());
				if (e.getEp() == null || e.getServiceId().equals(ed.getServiceId()))
					continue;
				if (Comparator.isSubstitute(e.getEp(), ed.getEp())) {
					substitutes.add(e);
				}
			}

		}
		if (substitutes.size() > 0) {
			int index = (int) (Math.random() * substitutes.size());
			return substitutes.get(index);
		} else {
			logger.debug("No replacements found for: " + ed.getServiceId());
			return null;
		}
	}

	public EventRepository getRepo() {
		return repo;
	}

	/**
	 * @param epId
	 * @return
	 * @throws Exception
	 * 
	 *             finds reusable nodes by querying the hierarchy
	 */
	private Map<String, List<String>> getReusableNodes(String epId) throws Exception {
		Map<String, List<String>> results = new HashMap<String, List<String>>();
		// TODO ADD identification of its own nodes in erh
		for (Entry entry : this.repo.getReusabilityHierarchy().getPrimitiveHierarchy().entrySet()) {
			if (((List<String>) entry.getKey()).contains(epId)) {
				List<String> codes = (List<String>) entry.getValue();
				for (String code : codes) {
					String node = code.split("\\|")[2];
					String replacement = code.split("\\|")[0].split(",")[0];
					String relation = code.split("\\|")[1];
					if (results.get(node) == null) {
						ArrayList<String> newlist = new ArrayList<String>();
						newlist.add(relation + "|" + replacement);
						results.put(node, newlist);
					} else {
						results.get(node).add(relation + "|" + replacement);
					}

				}
				// results.addAll((List<String>) entry.getValue());
				return results;
			}
		}
		return null;
	}

	private EventDeclaration getSubstituteEDforEP(EventPattern ep) throws CloneNotSupportedException,
			NodeRemovalException {
		if (ep.getSize() > 1)
			for (Entry e : this.repo.getEds().entrySet()) {
				EventDeclaration ed = ((EventDeclaration) e.getValue());
				if (ed.getEp() == null)
					continue;
				if (Comparator.isSubstitute(ed.getEp(), ep)) {// && !ed.getEp().getID().equals(ep.getID())
					return ed;
				}
			}
		else {
			for (Entry e : this.repo.getEds().entrySet()) {
				EventDeclaration ed = ((EventDeclaration) e.getValue());
				if (ed.getEp() != null)
					continue;
				if (Comparator.isSubstitutePrimitiveEvent(ep.getEds().get(0), ed)
						&& !ed.getServiceId().equals(ep.getEds().get(0).getServiceId())) {
					System.out.println("matching primitive event by type");
					return ed;
				}
			}
		}
		return null;
	}

	public List<EventPattern> getSubstituteEPforEP(EventPattern ep) {
		List<EventPattern> results = new ArrayList<EventPattern>();

		return results;
	}

	private boolean hasIRonRoot(EventPattern ep, Map<String, List<String>> reusabilities) throws Exception {
		// System.out.println("Root Id: " + ep.getRootId());
		// System.out.println("reusable: " + reusabilities);
		for (Entry en : reusabilities.entrySet()) {
			if (en.getKey().equals(ep.getRootId()))
				for (String s : ((List<String>) en.getValue()))
					if (s.contains("ir|"))
						return true;
		}
		return false;
	}

	/**
	 * @param replacements
	 * @return whether selected replacements has overlaps on their DSTs
	 * @throws NodeRemovalException
	 * @throws CloneNotSupportedException
	 */
	private boolean hasOverlap(List<String> replacements) throws CloneNotSupportedException, NodeRemovalException {
		for (int i = 0; i < replacements.size(); i++) {
			for (int j = 0; j < replacements.size(); j++) {
				if (i == j)
					continue;
				if (Comparator.getSameDSTs(repo.getEps().get(replacements.get(i)),
						repo.getEps().get(replacements.get(j))).size() != 0) {
					// EventPattern ep1 = repo.getEps().get(replacements.get(i));
					// EventPattern ep2 = repo.getEps().get(replacements.get(j));
					// System.out.println("has overlap: \n" + ep1.toSimpleString() + "\n" + ep2.toSimpleString());
					return true;
				}
			}
		}
		return false;
	}

	/**
	 * @param ep
	 * @param reusabilities
	 * @param parts
	 * @return
	 * @throws Exception
	 * 
	 *             creates a list of ACPs for the query from top-down
	 */
	private List<EventPattern> implementACP(EventPattern ep, Map<String, List<String>> reusabilities,
			Map<String, EventPattern> parts) throws Exception {
		// if (ep.getSize() == 6) {
		// logger.debug("==========================");
		// logger.debug("Implementing query acp: " + ep.toSimpleString());
		// }
		logger.debug("Implementing query acp: " + ep.toSimpleString());
		// if (ep.getID().contains("18103316-3f49-4767-8aa3-6b914410aa8b")) {
		// System.out.println("E!");
		// }
		List<EventPattern> results = new ArrayList<EventPattern>();
		EventDeclaration leaf = this.getSubstituteEDforEP(ep);// add leaf event declaration when there's a substitute
		if (leaf != null) {
			List<String> identicalEpIds = this.repo.getReusabilityHierarchy().getIdenticalEp(ep.getID());
			if (identicalEpIds.size() > 0) {
				String identicalEpId = identicalEpIds.get(0);
				if (!identicalEpId.equals(ep.getID())) {
					EventPattern identicalEp = this.repo.getEps().get(identicalEpId);
					return implementACP(identicalEp, reusabilities, parts);
				}
			}
			// logger.info("leaf not null " + results);
			EventPattern tempEP = new EventPattern();
			tempEP.setID("ACP-" + UUID.randomUUID());
			tempEP.getEds().add(leaf);
			if (leaf.getEp() != null) {
				List<Selection> sels = leaf.getEp().getSelections();
				for (Selection sel : sels) {
					Selection newSel = sel.clone();
					newSel.setProvidedBy(leaf.getnodeId());
					tempEP.getSelections().add(newSel);
				}
				// tempEP.setSelections(sels);
			}
			results.add(tempEP);
			logger.debug("leaf not null " + results);
			parts.put(tempEP.getID(), tempEP);

		}
		List<EventPattern> resultsWithoutIRonRoot = new ArrayList<EventPattern>();
		ArrayList<String> nodes = new ArrayList<String>();
		List<List<String>> replacementLists = new ArrayList<List<String>>();
		if (reusabilities != null)
			for (Entry e : reusabilities.entrySet()) {
				if (e.getKey().equals(ep.getRootId()))
					continue;
				nodes.add((String) e.getKey());
				String relation = (String) ((List) e.getValue()).get(0);
				List<String> replacements = new ArrayList<String>();
				List<EventPattern> drComponents = new ArrayList<EventPattern>();
				if (relation.contains("dr|")) {
					String epId = relation.split("\\|")[1];
					Map<String, List<String>> subReusabilities = this.getReusableNodes(epId);
					drComponents = this.implementACP(this.repo.getEps().get(epId), subReusabilities, parts);

				} else {
					EventPattern subPattern = ep.getSubtreeByRoot((String) e.getKey());
					drComponents = this.implementACP(subPattern, reusabilities, parts);
				}
				for (EventPattern epPart : drComponents) {
					parts.put(epPart.getID(), epPart);
					replacements.add(epPart.getID());
				}
				replacementLists.add(replacements);
			}
		replacementLists = this.cartesianProduct(replacementLists);
		logger.debug("Replacements: " + replacementLists);
		for (List<String> replacements : replacementLists) {
			resultsWithoutIRonRoot.add(this.createEPwithReplacements(ep, nodes, replacements, parts));

		}
		results.addAll(resultsWithoutIRonRoot);
		logger.debug("results with dr: " + results.size());
		if (this.hasIRonRoot(ep, reusabilities)) {
			if (ep.getSize() == 1) { // if ir on a single-node event pattern, the acps are its clone with replaced eds
				List<String> irReplacements = new ArrayList<String>();
				// List<List<String>> results = new ArrayList<List<String>>();
				for (Entry<String, List<String>> entry : reusabilities.entrySet()) {
					if (entry.getKey().equals(ep.getRootId())) {
						for (String reusable : entry.getValue()) {
							if (reusable.contains("ir|"))
								irReplacements.add(reusable.split("\\|")[1]);
						}
					}
				}
				for (String replacement : irReplacements) {
					EventPattern clone = ep.clone();
					clone.replaceED(ep.getRootId(), repo.getEDByEPId(replacement));
					results.add(clone);
				}
			} else {
				// logger.info("Implementing query with IR: ");
				List<List<String>> IRcombinations = getIRcombinations(ep, reusabilities);
				logger.debug("IR comb: " + IRcombinations);
				List<EventPattern> resultsWithIRonRoot = this.implementACPwithIR(ep, IRcombinations,
						resultsWithoutIRonRoot, parts);
				results.addAll(resultsWithIRonRoot);
				logger.debug("IR result size: " + resultsWithIRonRoot.size());
			}
		}
		// logger.info("Implementing query result: " + results);
		for (EventPattern resultEp : results) {
			logger.debug("----------------------");
			logger.debug("Reducing: " + resultEp);
			int nodeCnt1 = resultEp.getSize();
			resultEp = resultEp.getReducedPattern();
			int nodeCnt2 = resultEp.getSize();
			if (nodeCnt1 != nodeCnt2)
				logger.debug("Reduced: " + resultEp);
			else
				logger.debug("No change. ");
		}
		logger.debug("Results: " + results.size());
		return results;
	}

	/**
	 * @param iRcombinations
	 * @param resultsWithoutIRonRoot
	 * @return implement ACPs with the combination of in-direct reusable patterns on the root of ep
	 * @throws Exception
	 */
	private List<EventPattern> implementACPwithIR(EventPattern ep, List<List<String>> iRcombinations,
			List<EventPattern> resultsWithoutIRonRoot, Map<String, EventPattern> parts) throws Exception {
		logger.debug("Implementing ep with IR: " + ep.toSimpleString());
		// logger.info("IR Combinations " + iRcombinations.size());
		List<EventPattern> results = new ArrayList<EventPattern>();
		// System.out.println(resultsWithoutIRonRoot.size());
		// for (EventPattern ep1 : resultsWithoutIRonRoot)

		for (EventPattern partialResult : resultsWithoutIRonRoot) { // for each partial result without IR replacement,
																	// create acps with IR combinations
																	// System.out.println("Partial Result: " +
																	// partialResult);
			for (List<String> comb : iRcombinations) {
				EventPattern clone = partialResult.clone();
				List<List<String>> replacementLists = new ArrayList<List<String>>();
				List<String> nodes = new ArrayList<String>();
				for (String irReplacementId : comb) {
					// System.out.println("Partial Results: " + clone);
					List<String> replacements = new ArrayList<String>();
					EventPattern irReplacement = repo.getEps().get(irReplacementId);
					// System.out.println("IR replacement: " + irReplacement);
					// if (clone == null || irReplacement == null)
					// System.out.println("Null clone or replacement");
					List<String> dstsToremove = Comparator.getSameDSTs(clone, irReplacement);
					if (dstsToremove.size() == 0)
						logger.error("Empty Same Dsts: \n " + "Clone: " + clone + "\n" + "IrReplacement: "
								+ irReplacement);
					// System.out.println("dsts: " + dstsToremove);
					EventOperator rootClone = ((EventOperator) clone.getNodeById(clone.getRootId())).clone();
					rootClone.setID("tempRoot-" + UUID.randomUUID());
					nodes.add(rootClone.getID());
					clone.insertParent(rootClone, dstsToremove);
					List<EventPattern> irComponents = this.implementACP(irReplacement,
							this.getReusableNodes(irReplacementId), parts);
					// List<EventPattern> redundantIrComponents = new ArrayList<EventPattern>();
					for (EventPattern irComponent : irComponents) {
						if (irComponent.getSize() == irReplacement.getSize() && irReplacement.getSize() != 1)
							; // skip redundant eps
						else {
							parts.put(irComponent.getID(), irComponent);
							replacements.add(irComponent.getID());
						}
					}
					// irComponents.removeAll(redundantIrComponents);
					replacementLists.add(replacements);
					// EventPattern tempEp = new EventPattern();
					// tempEp.setID("ACPTemp");
					// tempEp.getEds().add(repo.getEDByEPId(irReplacementId));
					// clone.replaceSubtree("tempRoot", tempEp);

				}
				replacementLists = this.cartesianProduct(replacementLists);
				// System.out.println("Replacements: " + replacementLists);
				for (List<String> replacements : replacementLists) {
					results.add(this.createEPwithReplacements(clone, nodes, replacements, parts));
				}
				// clone.setID("ACP-" + UUID.randomUUID());
				// results.add(clone);
			}
		}
		return results;
	}

	public void setQuery(EventPattern query) {
		this.query = query;
	}

	public void setRepo(EventRepository repo) {
		this.repo = repo;
	}

}
