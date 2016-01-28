package org.insight_centre.aceis.engine;

import java.util.ArrayList;
import java.util.List;

import org.insight_centre.aceis.eventmodel.EventDeclaration;
import org.insight_centre.aceis.eventmodel.EventOperator;
import org.insight_centre.aceis.eventmodel.EventPattern;
import org.insight_centre.aceis.eventmodel.Filter;
import org.insight_centre.aceis.eventmodel.NodeRemovalException;
import org.insight_centre.aceis.eventmodel.OperatorType;
import org.insight_centre.aceis.eventmodel.QosVector;
import org.insight_centre.aceis.eventmodel.Selection;
import org.insight_centre.aceis.eventmodel.WeightVector;
import org.insight_centre.aceis.utils.Partitioner;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * @author feng
 * 
 *         provides a list of static methods for comparing the reusability between event patterns, qos utility
 *         calculation and others
 */
public class Comparator {
	public final static int direct = 1;
	public final static int indirect = 2;
	public final static int notreusable = 0;
	public final static int substitute = 3;
	public static final Logger logger = LoggerFactory.getLogger(Comparator.class);

	/**
	 * @param constraint
	 * @param weight
	 * @param capability
	 * @return utility calculated based on the given constraint, weight and capability
	 * @throws Exception
	 */
	public static Double calculateUtility(QosVector constraint, WeightVector weight, QosVector capability)
			throws Exception {
		// logger.info("Capability: " + capability.toString());
		// logger.info("constraint: " + constraint.toString());
		// logger.info("weight: " + weight.toString());
		double latencyFactor = (capability.getLatency() + 0.0) / (constraint.getLatency() + 0.0);
		if (latencyFactor > 1.0)
			throw new Exception("Latency constraint violated.");
		double priceFactor = (capability.getPrice() + 0.0) / (constraint.getPrice() + 0.0);
		if (priceFactor > 1.0)
			throw new Exception("Price constraint violated.");
		double trafficFactor = capability.getTraffic() / constraint.getTraffic();
		if (trafficFactor > 1.0)
			throw new Exception("Bandwidth consumption constraint violated.");
		double securityFactor = (capability.getSecurity() + 0.0) - (constraint.getSecurity() + 0.0);
		if (securityFactor < 0)
			throw new Exception("Security constraint violated.");
		double accuracyFactor = capability.getAccuracy() - constraint.getAccuracy();
		if (accuracyFactor < 0)
			throw new Exception("Accuracy constraint violated.");
		double reliabilityFactor = capability.getReliability() - constraint.getReliability();
		if (reliabilityFactor < 0)
			throw new Exception("Completeness constraint violated.");
		Double weightedLatency = weight.getLatencyW() * latencyFactor;
		Double weightedPrice = weight.getPriceW() * priceFactor;
		Double weightedTraffic = weight.getTrafficW() * trafficFactor;
		Double weightedSecurity = weight.getPriceW() * (securityFactor / (5.0 - constraint.getSecurity()));
		Double weightedAccuracy = weight.getAccuracyW() * (accuracyFactor / (1.0 - constraint.getAccuracy()));
		Double weightedReliability = weight.getReliabilityW()
				* (reliabilityFactor / (1.0 - constraint.getReliability()));

		double result = (weightedSecurity + weightedAccuracy + weightedReliability - weightedLatency - weightedPrice
				- weightedTraffic + 3) / 6;
		// logger.info("Utility: " + result);
		return result;
	}

	public static List<EventPattern> constraintEvaluation(List<EventPattern> results, QosVector constraint)
			throws CloneNotSupportedException, NodeRemovalException {
		List<EventPattern> filteredResults = new ArrayList<EventPattern>();
		for (EventPattern ep : results) {
			if (ep.aggregateQos().satisfyConstraint(constraint))
				filteredResults.add(ep);
		}
		return filteredResults;
	}

	/**
	 * @param ep1
	 * @param ep2
	 * @return
	 * @throws CloneNotSupportedException
	 * @throws NodeRemovalException
	 * 
	 *             checks if two event patterns are directly reusable
	 */
	public static boolean directReusable(EventPattern ep1, EventPattern ep2) throws CloneNotSupportedException,
			NodeRemovalException {// ep1 reuses ep2
		if (isSubstitute(ep1, ep2))
			return true;
		return false;
	}

	/**
	 * @param ep1
	 * @param ep2
	 * @param rootOpt
	 * @return
	 * @throws CloneNotSupportedException
	 * @throws NodeRemovalException
	 * 
	 *             get the list of DSTs in ep1 shared by ep2
	 */
	public static List<String> getSameDSTs(EventPattern ep1, EventPattern ep2) throws NodeRemovalException,
			CloneNotSupportedException {
		List<EventPattern> dstList1 = ep1.getDirectSubtrees();
		List<EventPattern> dstList2 = ep2.getDirectSubtrees();
		List<String> results = new ArrayList<String>();
		for (int i = 0; i < dstList1.size(); i++)
			for (int j = 0; j < dstList2.size(); j++)
				if (Comparator.isSubstitute(dstList1.get(i), dstList2.get(j)))
					results.add(dstList1.get(i).getRootId());
		return results;

		// return false;
	}

	/**
	 * @param ep1
	 * @param ep2
	 * @param rootOpt
	 * @return
	 * @throws CloneNotSupportedException
	 * @throws NodeRemovalException
	 * 
	 *             checks if two event patterns have the same set of direct sub-trees
	 */
	private static boolean hasSameDSTs(EventPattern ep1, EventPattern ep2, OperatorType rootOpt)
			throws CloneNotSupportedException, NodeRemovalException {
		// boolean result = false;
		List<EventPattern> dstList1 = ep1.getDirectSubtrees();
		List<EventPattern> dstList2 = ep2.getDirectSubtrees();
		if (dstList1.size() != dstList2.size())
			return false;
		if (rootOpt == OperatorType.seq || rootOpt == OperatorType.rep) {
			// for(int i=0;i<dstList1.s)
			// System.out.println("Sorting from comparator");
			dstList1 = Comparator.sortDSTs(dstList1, ep1);
			// System.out.println("Sorting from comparator");
			dstList2 = Comparator.sortDSTs(dstList2, ep2);
			for (int i = 0; i < dstList1.size(); i++) {
				if (!Comparator.isSubstitute(dstList1.get(i), dstList2.get(i)))
					return false;
			}
			return true;
		} else {
			for (int i = 0; i < dstList1.size(); i++) {
				EventPattern tempEp1 = dstList1.get(i);
				boolean hasSubstitute = false;
				for (int j = 0; j < dstList2.size(); j++) {
					if (Comparator.isSubstitute(tempEp1, dstList2.get(j))) {
						hasSubstitute = true;
						break;
					}
				}
				if (!hasSubstitute)
					return false;
			}
			return true;
		}
		// return false;
	}

	private static boolean hasSameFiltersOnRoot(EventPattern ep1, EventPattern ep2) {
		if (ep1.getFilters() == null && ep2.getFilters() == null)
			return true;
		else if (ep1.getFilters() != null && ep2.getFilters() == null)
			return false;
		else if (ep1.getFilters() == null && ep2.getFilters() != null)
			return false;
		else {
			List<Filter> filters1 = ep1.getFilters().get(ep1.getRootId());
			List<Filter> filters2 = ep2.getFilters().get(ep2.getRootId());
			if (filters1 == null && filters2 == null)
				return true;
			else if ((filters1 == null && filters2 != null) || (filters1 != null && filters2 == null))
				return false;
			else {
				if (filters1.size() != filters2.size())
					return false;
				for (Filter f1 : filters1) {
					boolean compatibleFilter = false;
					for (Filter f2 : filters2) {
						if (f1.equals(f2)) {
							compatibleFilter = true;
							break;
						}
					}
					if (compatibleFilter)
						continue;
					else
						return false;
				}
				return true;
			}
		}
	}

	private static boolean hasSameSelectionsOnRoot(EventPattern ep1, EventPattern ep2) {
		List<Selection> sel1 = ep1.getSelectionOnNode(ep1.getRootId());
		List<Selection> sel2 = ep2.getSelectionOnNode(ep2.getRootId());
		if (sel1.size() == 0 && sel2.size() == 0)
			return true;
		else if (sel1.size() == sel2.size()) {
			EventDeclaration ed1 = (EventDeclaration) ep1.getNodeById(ep1.getRootId());
			EventDeclaration ed2 = (EventDeclaration) ep2.getNodeById(ep2.getRootId());
			if (ed1.getEventType().equals(ed2.getEventType())) {
				for (Selection sel11 : sel1) {
					boolean hasSameSelection = false;
					for (Selection sel22 : sel2) {
						if (sel11.getFoi().equals(sel22.getFoi())
								&& sel11.getPropertyType().equals(sel22.getPropertyType())) {
							hasSameSelection = true;
							break;
						}
					}
					if (!hasSameSelection)
						return false;
				}
				return true;
			} else
				return false;
		} else
			return false;
	}

	/**
	 * @param ep1
	 * @param ep2
	 * @return
	 * @throws CloneNotSupportedException
	 * @throws NodeRemovalException
	 * 
	 *             checks if two event patterns are in-directly reusable
	 */
	public static boolean inDirectReusable(EventPattern ep1, EventPattern ep2) throws CloneNotSupportedException,
			NodeRemovalException {
		// System.out.println("Comparing: " + ep1.toString() + " to " + ep2.toString());
		//
		if (ep1 == null || ep2 == null || ep1.getSize() == 0 || ep2.getSize() == 0)
			return false;
		if (ep1.getSize() == 1 && ep2.getSize() == 1) {
			if (ep1.getEds().get(0).getEventType().equals(ep2.getEds().get(0).getEventType())
					&& ep1.getEds().get(0).getFoi().equals(ep2.getEds().get(0).getFoi())
					&& rootFiltersCovered(ep1, ep2) && rootSelectionsCovered(ep1, ep2))
				return true;
			else
				return false;
		}
		EventOperator root1 = (EventOperator) ep1.getNodeById(ep1.getRootId());
		EventOperator root2 = (EventOperator) ep2.getNodeById(ep2.getRootId());
		if (root1.getOpt() != root2.getOpt())
			if (!(root1.getOpt() == OperatorType.rep && root2.getOpt() == OperatorType.seq))
				return false;
		if (!rootFiltersCovered(ep1, ep2))
			return false;
		List<EventPattern> dstList1 = ep1.getDirectSubtrees();
		List<EventPattern> dstList2 = ep2.getDirectSubtrees();
		if (root2.getOpt() == OperatorType.seq) {
			if (dstList1.size() >= dstList2.size()) {
				dstList1 = sortDSTs(dstList1, ep1);
				dstList2 = sortDSTs(dstList2, ep2);
				int index = -1;
				// boolean isSubsequence = true;
				for (EventPattern dst2 : dstList2) {
					// if (!isSubsequence)
					// return false;
					if (index < 0) {
						boolean hasSub = false;
						for (int i = 0; i < dstList1.size(); i++) {
							EventPattern dst1 = dstList1.get(i);
							if (isSubstitute(dst1, dst2)) {
								if ((dstList1.size() - i) < dstList2.size())
									return false;
								index = i;
								hasSub = true;
								break;
							}
						}
						if (!hasSub)
							return false;
						else
							continue;
					}
					EventPattern dst12 = dstList1.get(index + 1);
					if (isSubstitute(dst2, dst12)) {
						index += 1;
					} else
						return false;
				}
				return true;
			}
			return false;
		} else if (root2.getOpt() == OperatorType.rep) {
			int card1 = root1.getCardinality();
			int card2 = root2.getCardinality();
			if (card1 > card2)
				if (Partitioner.getFactors(card1).contains(card2))
					if (hasSameDSTs(ep1, ep2, OperatorType.rep))
						return true;

			return false;
		} else {
			if (dstList1.size() >= dstList2.size()) {
				for (EventPattern dst2 : dstList2) {
					boolean hasSame = false;
					for (EventPattern dst1 : dstList1) {
						if (isSubstitute(dst1, dst2)) {
							hasSame = true;
							break;
						}
					}
					if (!hasSame)
						return false;
				}
				return true;
			}
			return false;
		}
		// return false;

	}

	/**
	 * @param ep1
	 * @param ep2
	 * @return
	 * @throws Exception
	 * 
	 *             checks if the canonical event patterns of two event patterns are substitutes to each other
	 */
	public static boolean isCanonicalSubstitute(EventPattern ep1, EventPattern ep2) throws Exception {
		// System.out.println("Canonical Comparing: " + ep1.toString() + "\n" + ep2.toString());
		return isSubstitute(ep1.getCanonicalPattern(), ep2.getCanonicalPattern());
	}

	public static boolean isSubstitute(EventPattern ep1, EventPattern ep2) throws CloneNotSupportedException,
			NodeRemovalException {
		if (ep1.getSize() != ep2.getSize() || ep1.getHeight() != ep2.getHeight())
			return false;
		if (ep1 == null || ep2 == null || ep1.getSize() == 0 || ep2.getSize() == 0)
			return false;
		if (ep1.getSize() == 1 && ep2.getSize() == 1) {
			EventDeclaration ed1 = ep1.getEds().get(0);
			EventDeclaration ed2 = ep2.getEds().get(0);
			boolean isSubstitutePrimitiveEvent = isSubstitutePrimitiveEvent(ed1, ed2);
			boolean hasSameFiltersOnRoot = hasSameFiltersOnRoot(ep1, ep2);
			boolean hasSameSelectionsOnRoot = hasSameSelectionsOnRoot(ep1, ep2);
			if (isSubstitutePrimitiveEvent && hasSameFiltersOnRoot && hasSameSelectionsOnRoot)
				return true;
			return false;
		}

		EventOperator root1 = (EventOperator) ep1.getNodeById(ep1.getRootId());
		EventOperator root2 = (EventOperator) ep2.getNodeById(ep2.getRootId());
		// System.out.println("Comparing: " + ep1.toString() + ", " + ep2.toString() + " size: " + ep1.getSize() + ", "
		// + ep2.getSize());
		boolean sameOp = (root1.getOpt() == root2.getOpt()) && (root1.getCardinality() == root2.getCardinality())
				&& hasSameFiltersOnRoot(ep1, ep2);
		// List<EventPattern> dstList1 = ep1.getDirectSubtrees();
		// List<EventPattern> dstList2 = ep2.getDirectSubtrees();
		if (sameOp) {
			OperatorType rootOpt = root1.getOpt();
			boolean sameDST = Comparator.hasSameDSTs(ep1, ep2, rootOpt);
			// boolean sameFilters = Comparator.hasSameFilters(ep1, ep2);
			if (sameDST)
				return true;
		}
		return false;
	}

	public static boolean isSubstitutePrimitiveEvent(EventDeclaration ed1, EventDeclaration ed2) {
		// System.out.println("Comparing:\n" + ed1 + "\n" + ed2);
		if (ed1.getFoi() == null || ed1.getEventType() == null)
			System.out.println("null foi or type:" + ed1);
		if (ed2.getFoi() == null || ed2.getEventType() == null)
			System.out.println("null foi or type:" + ed2);
		if (ed1.getEventType().equals(ed2.getEventType()) && isProximityFoI(ed1.getFoi(), ed2.getFoi()))
			return true;
		return false;
	}

	private static boolean isProximityFoI(String foi1, String foi2) {
		if (foi1.equals(foi2))
			return true;
		else {
			if (foi1.split("-").length == foi2.split("-").length) {
				if (foi1.split("-").length == 1) {
					double lat1 = Double.parseDouble(foi1.split(",")[0]);
					double long1 = Double.parseDouble(foi1.split(",")[1]);
					double lat2 = Double.parseDouble(foi2.split(",")[0]);
					double long2 = Double.parseDouble(foi2.split(",")[1]);
					if (lat1 - lat2 <= 0.0000000001 && lat1 - lat2 >= -0.0000000001 && long1 - long2 <= 0.0000000001
							&& long1 - long2 >= -0.0000000001)
						return true;
				} else if (foi1.split("-").length > 1) {
					boolean result = true;
					String[] coordinates1 = foi1.split("-");
					String[] coordinates2 = foi2.split("-");
					for (int i = 0; i < coordinates1.length; i++) {
						result = result && isProximityFoI(coordinates1[i], coordinates2[i]);
					}
					return result;
				}
			}
		}
		return false;
	}

	public static void main(String[] args) throws CloneNotSupportedException, NodeRemovalException {
		// String p1 = "EP34(1,0)=or:1-0(rep:8-1(light0:1.0-2,windspeed1:2.0-3),humidity1:2.0-4)";
		// String p2 = "EP34(1,0)=rep:8-0(light0:1.0-1,windspeed1:2.0-2)";
		// EventPattern ep1 = TextFileManager.stringToPattern(p1);
		// EventPattern ep2 = TextFileManager.stringToPattern(p2);
		// System.out.println(Comparator.reusable(ep1, ep2));
	}

	/**
	 * @param ep1
	 * @param ep2
	 * @return
	 * @throws CloneNotSupportedException
	 * @throws NodeRemovalException
	 * 
	 *             derive the reusability code for two event patterns
	 */
	public static String reusable(EventPattern ep1, EventPattern ep2) throws CloneNotSupportedException,
			NodeRemovalException {// ep1 reuses ep2
		if (ep1.getSize() > ep2.getSize()) {
			List<EventPattern> possibleReuse = ep1.getSubtreesByHeight(ep2.getHeight());
			for (EventPattern ep : possibleReuse) {
				if (directReusable(ep, ep2)) {
					return "dr|" + ep.getRootId();
				} else if (inDirectReusable(ep, ep2)) {
					return "ir|" + ep.getRootId();
				}
			}
			return "nr";
		} else if (ep1.getSize() == ep2.getSize()) {
			if (isSubstitute(ep1, ep2)) {
				return "sub|" + ep1.getRootId();
			} else if (inDirectReusable(ep1, ep2)) {
				return "ir|" + ep1.getRootId();
			} else if (inDirectReusable(ep2, ep1)) {
				return "-ir|" + ep2.getRootId();
			} else
				return "nr";
		} else {
			return "-" + reusable(ep2, ep1);
		}
	}

	private static boolean rootFiltersCovered(EventPattern ep1, EventPattern ep2) {
		if (ep2.getFilters() == null)
			return true;
		else if (ep1.getFilters() == null && ep2.getFilters() != null)
			return false;
		else {
			List<Filter> rootFilters1 = ep1.getFilters().get(ep1.getRootId());
			List<Filter> rootFilters2 = ep2.getFilters().get(ep2.getRootId());
			if (rootFilters2 == null)
				return true;
			else if (rootFilters1 == null && rootFilters2 != null)
				return false;
			else {
				for (Filter f2 : rootFilters2) {
					// boolean filterCovers = false;
					List<Filter> compatibleFilters = f2.getCompatible(rootFilters1);
					if (compatibleFilters.size() == 0 || f2.covers(compatibleFilters.get(0)))
						continue;
					else
						return false;
				}
				return true;
			}
		}
		// return false;
	}

	public static boolean rootSelectionsCovered(EventPattern ep1, EventPattern ep2) {
		List<Selection> sel1 = ep1.getSelectionOnNode(ep1.getRootId());
		List<Selection> sel2 = ep2.getSelectionOnNode(ep2.getRootId());
		if (ep1.getSize() == 1 && ep2.getSize() == 1)
			if (sel2.size() == 0)
				return true;
		if (sel1.size() < sel2.size()) {
			for (Selection sel11 : sel1) {
				boolean hasSub = false;
				for (Selection sel22 : sel2) {
					if (sel11.substitues(sel22))
						hasSub = true;
				}
				if (!hasSub)
					return false;
			}
			return true;
		} else if (sel2.size() == 0) // empty selection means all properties selected
			return true;
		return false;
	}

	/**
	 * @param dstList
	 * @param parentEp
	 * @return
	 * @throws CloneNotSupportedException
	 * @throws NodeRemovalException
	 * 
	 *             sort the direct sub-trees for a given event pattern based on their temporal relations
	 */
	public static List<EventPattern> sortDSTs(List<EventPattern> dstList, EventPattern parentEp)
			throws CloneNotSupportedException, NodeRemovalException {
		// debug!!!
		List<EventPattern> results = new ArrayList<EventPattern>();
		List<String> dstRoots = new ArrayList<String>();
		for (int i = 0; i < dstList.size(); i++) {
			dstRoots.add(dstList.get(i).getRootId());
		}
		// System.out.println("unsorted: " + dstRoots + " dst size: " + dstList.size());
		// System.out.println("Sorting from merge:");
		List<String> sortedDSTRoots = parentEp.sortSequenceChilds(dstRoots);
		// System.out.println("sorted: " + sortedDSTRoots);
		for (int i = 0; i < sortedDSTRoots.size(); i++) {
			results.add(parentEp.getSubtreeByRoot(sortedDSTRoots.get(i)));
		}
		return results;
	}
}
