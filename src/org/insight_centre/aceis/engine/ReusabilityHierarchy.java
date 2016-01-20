package org.insight_centre.aceis.engine;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;

import org.insight_centre.aceis.eventmodel.EventPattern;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * @author feng
 * 
 *         pattern-based event composition module, also takes care of the reusability hierarchy construction and
 *         maintenance
 */
public class ReusabilityHierarchy implements Cloneable {

	private static boolean critical = false; // debugging flagsFs
	public static final Logger logger = LoggerFactory.getLogger(ReusabilityHierarchy.class);
	private List<EventPattern> candidates;

	private Map<List<String>, List<String>> hierarchy;

	private List<List<String>> identical;
	private List<EventPattern> queries;
	private List<String> roots;

	public ReusabilityHierarchy() {
		super();
		this.queries = new ArrayList<EventPattern>();
		this.candidates = new ArrayList<EventPattern>();
		this.hierarchy = new HashMap<List<String>, List<String>>();
		this.roots = new ArrayList<String>();
		this.identical = new ArrayList<List<String>>();
	}

	public ReusabilityHierarchy clone() throws CloneNotSupportedException {
		return (ReusabilityHierarchy) super.clone();

	}

	public ReusabilityHierarchy(List<EventPattern> queries, List<EventPattern> candidates) {
		super();
		this.queries = queries;
		this.candidates = candidates;
		this.hierarchy = new HashMap<List<String>, List<String>>();
		this.roots = new ArrayList<String>();
		this.identical = new ArrayList<List<String>>();
	}

	private void addEdge(String parent, String current, String reusability) throws Exception {
		// if (!insertingParent) {
		// if (parent == null) {
		// List<String> newKey = new ArrayList<String>();
		// newKey.add(current);
		// this.hierarchy.put(newKey, new ArrayList<String>());
		// } else {
		if (!hasEdge(parent, current)) {
			if (critical)
				logger.info("Adding edge: " + this.getEpById(parent).toSimpleString() + ", "
						+ this.getEpById(current).toSimpleString());
			String reusedIds = "";
			if (this.getIdenticalEp(current).size() == 0)
				reusedIds = current;
			else {
				for (String s : this.getIdenticalEp(current))
					reusedIds += s + ",";
				reusedIds = reusedIds.substring(0, reusedIds.length() - 1);
			}
			for (Map.Entry en : this.hierarchy.entrySet()) {
				if (((List) en.getKey()).contains(parent)) {
					((List) en.getValue()).add(reusedIds + "|" + reusability);
					break;
				}
			}
		}
		// }
		// } else {
		//
		// }

	}

	public void buildHierarchy() throws Exception {
		long t1 = System.currentTimeMillis();
		logger.info("Building Event Reusability Hierarchy");
		for (EventPattern ep : this.candidates)
			insertIntoHierarchy(ep);

		long t2 = System.currentTimeMillis();
		logger.info("Event Reusability Hierarchy built in: " + (t2 - t1) + " ms with "
				+ this.hierarchy.entrySet().size() + " merged nodes.");
		// this.printERH();
	}

	// private boolean analyzePartitions(EventPattern query, String queryRoot) throws Exception {
	// EventOperator root = (EventOperator) query.getNodeById(queryRoot);
	// OperatorType rootOpt = root.getOpt();
	// ArrayList partitions = new ArrayList();
	// if (rootOpt == OperatorType.seq)
	// partitions = Partitioner.getOrderedPartition((ArrayList) query.getChildIds(queryRoot));
	// else
	// partitions = Partitioner.getUnorderedPartition((ArrayList) query.getChildIds(queryRoot));
	// // System.out.println(partitions);
	// HashMap<Object, EventPattern> bestPartitionMap = new HashMap();
	// Double leastTrafficDemand = -1.0;
	// for (int i = 0; i < partitions.size(); i++) {
	// Object partition = partitions.get(i);
	// boolean matchingFailed = false;
	//
	// HashMap<Object, EventPattern> partMap = new HashMap();
	// for (Object part : (ArrayList) partition) {
	// EventPattern tempTree = new EventPattern("temp" + "-sub", new ArrayList<EventDeclaration>(),
	// new ArrayList<EventOperator>(), new HashMap<String, List<String>>(),
	// new HashMap<String, String>(), 1, -1);
	//
	// List<EventPattern> tempSubTrees = new ArrayList<EventPattern>();
	// for (Object s : (ArrayList) part) {
	// tempSubTrees.add(query.getSubtreeByRoot((String) s));
	// }
	// if (tempSubTrees.size() > 1) {
	// EventOperator tempRoot = new EventOperator(rootOpt, root.getCardinality(), "0");
	// tempTree.getEos().add(tempRoot);
	// tempTree.getProvenanceMap().put(tempRoot.getID(), new ArrayList<String>());
	// tempTree.addDSTs(tempSubTrees, tempRoot);
	// } else if (tempSubTrees.size() == 1) {
	// tempTree = tempSubTrees.get(0);
	// }
	//
	// EventPattern tempSub = this.getSubstituteWithMinimalFreq(tempTree);
	// if (tempSub == null) {
	// matchingFailed = true;
	// break;
	// } else
	// partMap.put(part, tempSub);
	// }
	// if (matchingFailed)
	// continue;// if any matching for a part in the partition fails, continue
	// else {
	// Double traffic = computeTrafficForPartition(partMap, rootOpt);
	// if (leastTrafficDemand < 0.0 || traffic < leastTrafficDemand) {
	// leastTrafficDemand = traffic;
	// bestPartitionMap = partMap;
	// }
	// }
	// }
	// if (bestPartitionMap.size() <= 0) {// all partition failed to find matches
	// return false;
	// } else {// replace substitutes according to partition map
	// Iterator<Entry<Object, EventPattern>> it = bestPartitionMap.entrySet().iterator();
	// while (it.hasNext()) {
	// Entry<Object, EventPattern> e = it.next();
	// ArrayList<String> part = (ArrayList<String>) e.getKey();
	// EventPattern replacement = e.getValue();
	// EventOperator newSeq = new EventOperator(rootOpt, root.getCardinality(), query.getIdCnt() + 1 + "");
	// query.insertParent(newSeq, part);
	// replacePattern(newSeq.getID(), replacement, query);
	// }
	// return true;
	// }
	//
	// }

	public List<String> getAllDescendants(String epId) {
		// logger.info("getting descendants for: " + epId);
		// this.printERH();
		Set<String> decendants = this.getDescendants(epId);
		List<String> results = new ArrayList<String>();
		for (String s : decendants) {
			results.addAll(this.getIdenticalEp(s));
		}
		return results;

	}

	// public void print
	// private Double computeTrafficForPartition(HashMap<Object, EventPattern> partMap, OperatorType opt) {
	// Double result = -1.0;
	// if (opt == OperatorType.or) {
	// result = 0.0;
	// for (EventPattern replacement : partMap.values()) {
	// result += replacement.getFrequency();
	// }
	// } else {
	// for (EventPattern replacement : partMap.values()) {
	// Double freq = replacement.getFrequency();
	// if (result < 0 || freq < result) {
	// result = freq;
	// }
	// }
	// }
	// return result;
	// }

	public List<EventPattern> getCandidates() {
		return candidates;
	}

	@SuppressWarnings({ "rawtypes", "unchecked" })
	private List<String> getChildNodes(String root) {
		// System.out.println("Getting children for: " + root);
		List<String> result = new ArrayList<String>();
		for (Map.Entry en : this.hierarchy.entrySet()) {
			if (((List) en.getKey()).contains(root)) {
				List<String> values = (List<String>) en.getValue();
				for (String value : values) {
					result.add(value.split("\\|")[0].split(",")[0]);
					// System.out.println("getting childnodes: " + value.split("\\|")[0].split(",")[0]);
				}
			}
		}
		// System.out.println("Children : " + result);
		return result;
	}

	private Set<String> getDescendants(String epId) {
		HashSet<String> results = new HashSet<String>();
		List<String> childNodes = this.getChildNodes(epId);
		results.addAll(childNodes);
		for (String s : childNodes) {
			results.addAll(this.getDescendants(s));
		}
		return results;
	}

	private EventPattern getEpById(String id) {
		for (EventPattern ep : this.candidates)
			if (ep.getID().equals(id))
				return ep;
		return null;
	}

	// public List<String> getCompositionByReuse(EventPattern query) throws CloneNotSupportedException,
	// NodeRemovalException {
	// List<String> results = new ArrayList<String>();
	// Set<String> visited = new HashSet<String>();
	// this.reusabilitySearch(query, this.roots, results, visited);
	// return results;
	// }

	// public EventPattern getCompositionBySubstitution(EventPattern query) throws Exception {
	//
	// String queryRoot = query.getRootId();
	// List<EventPattern> results = new ArrayList<EventPattern>();
	// boolean hasReplacement = false;
	// this.substituteTraverse(query, queryRoot, hasReplacement);
	// // System.out.println("Has substitutes: " + hasReplacement);
	// return query;
	// }

	public List<List<String>> getIdentical() {
		return identical;
	}

	public List<String> getIdenticalEp(String epId) {
		List<String> results = new ArrayList<String>();
		for (List<String> list : this.identical) {
			if (list.contains(epId))
				results = list;
		}
		return results;
	}

	private List<String> getLeavesFromHierarchy() {
		List<String> valueSet = new ArrayList<String>();

		// List<String> results = new ArrayList<String>();
		if (this.hierarchy == null)
			return valueSet;
		// List<String> possibleRoots = new ArrayList<String>();
		for (Map.Entry en : this.hierarchy.entrySet()) {
			if (((List) en.getValue()).size() == 0) {
				String key = ((List<String>) en.getKey()).get(0);
				if (!valueSet.contains(key))
					valueSet.add(key);
			}
		}
		// for (String id : valueSet) {
		// // String id = ep.getID();
		// boolean hasChild = false;
		// for (Map.Entry en : this.hierarchy.entrySet()) {
		// List<String> keys = (List<String>) en.getKey();
		// if (keys.contains(id) && ((List) en.getValue()).size() > 0)
		// hasChild = true;
		// if (hasChild)
		// break;
		// }
		// if (!hasChild)
		// results.add(id);
		// }
		// System.out.println("Leaves: " + valueSet);
		return valueSet;
	}

	private List<String> getParentNodes(String root) {
		// System.out.println("Getting parents for: " + root);
		List<String> result = new ArrayList<String>();
		for (Map.Entry en : this.hierarchy.entrySet()) {
			List<String> values = (List<String>) en.getValue();
			List<String> keys = (List<String>) en.getKey();
			for (String value : values) {
				if (Arrays.asList(value.split("\\|")[0].split(",")).contains(root)) {
					result.add(keys.get(0));
				}
			}
		}
		// System.out.println("Parents: " + result);
		return result;
	}

	public Map<List<String>, List<String>> getPrimitiveHierarchy() {
		return hierarchy;
	}

	// private List<EventPattern> getSubstitutes(EventPattern query) throws CloneNotSupportedException,
	// NodeRemovalException {
	// List<EventPattern> results = new ArrayList<EventPattern>();
	// for (EventPattern ep : this.candidates) {
	// if (Comparator.isSubstitute(ep, query))
	// results.add(ep);
	// }
	// return results; // empty list if no match
	// }

	public List<EventPattern> getQueries() {
		return queries;
	}

	public List<String> getRootsFromHierarchy() {
		Set<String> keySet = new HashSet<String>();

		List<String> results = new ArrayList<String>();
		if (this.hierarchy == null)
			return results;
		// List<String> possibleRoots = new ArrayList<String>();
		for (Map.Entry en : this.hierarchy.entrySet()) {
			if (!keySet.contains(((List) en.getKey()).get(0)))
				keySet.add((String) ((List) en.getKey()).get(0));
		}
		for (String id : keySet) {
			// String id = ep.getID();
			boolean hasParent = false;
			for (Map.Entry en : this.hierarchy.entrySet()) {
				List<String> values = (List<String>) en.getValue();
				for (String s : values) {
					if (Arrays.asList(s.split("\\|")[0].split(",")).contains(id)) {
						hasParent = true;
						break;
					}
				}
				if (hasParent)
					break;
			}
			if (!hasParent)
				results.add(id);
		}
		return results;
	}

	private boolean hasEdge(String parent, String child) throws Exception {
		if (parent == null || child == null)
			throw new Exception("Error: adding edge with null parent or child.");
		for (Entry e : this.hierarchy.entrySet()) {
			List<String> keys = (List<String>) e.getKey();
			List<String> values = (List<String>) e.getValue();
			if (keys.contains(parent)) {
				for (String value : values) {
					List<String> reusedIds = Arrays.asList(value.split("\\|")[0].split(","));
					if (reusedIds.contains(child))
						return true;
				}
			}
		}
		return false;
	}

	public void insertIntoHierarchy(EventPattern ep) throws Exception {
		// if (ep.getID().contains("Request")) {
		// critical = true;
		// }
		if (critical)
			logger.info("\n ===inserting: " + ep.toSimpleString() + "=========");
		// if (ep.getID().contains("d51c945fd0da"))
		// System.out.println("E!");
		if (critical)
			for (Entry e : hierarchy.entrySet())
				logger.debug("ERH b4: " + toSimpleEntryString(e));
		if (!this.candidates.contains(ep))// if inserting from outside, add it to the candidates;
			this.candidates.add(ep);
		ArrayList<String> visited = new ArrayList<String>();
		List<String> newKey = new ArrayList<String>();

		newKey.add(ep.getID());
		hierarchy.put(newKey, new ArrayList<String>());
		visited.add(ep.getID());
		List<String> roots = this.getRootsFromHierarchy();
		for (String root : roots) {// TODO it is inefficient to invoke getRoots method every
			// time

			this.reusabilityTraverse(ep, root, null, visited);

		}
		// if (ep.getID().contains("Request"))
		// System.out.println("V: " + visited);
		// boolean merged = true;
		if (this.getIdenticalEp(ep.getID()).size() == 0) {
			// merged = false;
			logger.debug("Not merged: " + ep.getID());
			for (String leaf : this.getLeavesFromHierarchy()) {
				this.reversedReusabilityTraverse(ep, leaf, null, visited);
			}
		}
		// if (ep.getID().contains("Request"))
		// System.out.println("V a: " + visited);
		// Simulator2.logger.write("----------------------\n");
		// Simulator2.logger.write("after: \n");
		// System.out.println("-----------------------");
		// for (List<String> list : this.identical)
		// if (!merged) {
		// logger.debug("---------------------------- ");

		// if (critical) {
		// for (Entry e : this.getPrimitiveHierarchy().entrySet()) {
		// List<String> keys = (List<String>) e.getKey();
		// List<String> values = (List<String>) e.getValue();
		// List<String> childNodes = new ArrayList<String>();
		// for (String value : values) {
		// childNodes.add(value.split("\\|")[0].split("\\,")[0]);
		// if (!value.contains(keys.get(0))) {
		// logger.error("ERH inconsistency found.");
		// break;
		// }
		// }
		// boolean consistent = true;
		// for (int i = 0; i < childNodes.size(); i++) {
		// for (int j = 0; j < childNodes.size(); j++) {
		// if (i == j)
		// continue;
		// String reusability = Comparator.reusable(this.getEpById(childNodes.get(i)),
		// this.getEpById(childNodes.get(j)));
		// String code = reusability.split("\\|")[0];
		// if (!code.contains("nr")) {
		// logger.info("Inconsistent ERH: " + toSimpleEntryString(e));
		// logger.info("caused by: " + this.getEpById(childNodes.get(i)).toSimpleString() + " and "
		// + this.getEpById(childNodes.get(j)).toSimpleString());
		// logger.info("reusability: " + reusability);
		// consistent = false;
		// break;
		// }
		// }
		// if (!consistent)
		// break;
		// }
		// if (!consistent) {
		// logger.info("ERH after: " + toSimpleEntryString(e) + "\n");
		// System.exit(0);
		// }
		// }
		//
		// }

		// }
		// for (Entry e : this.getHierarchy().entrySet())
		// Simulator2.logger.write("ERH: " + toSimpleEntryString(e) + "\n");
	}

	private boolean mergeIdenticalPatterns(String existing, String inserted) throws IOException {
		// Simulator2.logger.write("============================\n");
		// logger.info("Merging: " + this.getEpById(existing).toSimpleString() + " with: "
		// + this.getEpById(inserted).toSimpleString() + "\n");
		// System.out.println("Merging: " + this.getEpById(existing).toSimpleString() + ", "
		// + this.getEpById(inserted).toSimpleString() + "\n");
		// Simulator2.logger.write("before: --------------\n");
		// for (Entry e : this.getHierarchy().entrySet())
		// Simulator2.logger.write("ERH before: " + this.toSimpleEntryString(e) + "\n");
		// if (parent == null) {
		// System.out.println("Merging: " + this.getEpById(existing).toSimpleString() + ", "
		// + this.getEpById(inserted).toSimpleString());
		if (this.getIdenticalEp(existing).contains(inserted))
			return false;// already merged
		if (this.getIdenticalEp(existing).size() == 0) {
			List<String> tempList = this.getIdenticalEp(existing);
			tempList.add(existing);
			this.identical.add(tempList);
		}
		this.getIdenticalEp(existing).add(inserted);

		List<String> keyToRemove = new ArrayList<String>();
		for (Map.Entry en : this.hierarchy.entrySet()) {
			List<String> key = (List) en.getKey();
			if (key.contains(inserted) && key.size() == 1) {
				keyToRemove = key; // remove the redundant entry
				continue;
				// break;
			} else if (key.contains(existing)) {
				key.add(inserted); // add inserted pattern into existing entry keys
				continue;
			}
			List<String> values = (List<String>) en.getValue();
			List<String> valueToRemove = new ArrayList<String>();
			String newValue = "";
			for (String value : values) {
				List<String> reusedIds = Arrays.asList((value.split("\\|")[0].split(",")));
				if (reusedIds.contains(existing) && !reusedIds.contains(inserted)) {
					valueToRemove.add(value);
					String firstSeg = value.split("\\|")[0];
					String rest = value.substring(firstSeg.length());
					newValue = firstSeg + "," + inserted + rest;
					// break;
				} else if (reusedIds.contains(inserted) && reusedIds.size() == 1)
					valueToRemove.add(value);

			}
			if (valueToRemove.size() > 0) {
				// System.out.println("removing values size: " + values.size());
				((List) en.getValue()).removeAll(valueToRemove); // remove old values without inserted pattern or the
				// System.out.println("removed values size: " + values.size()); // value contains only inserted
			}
			if (!newValue.equals("")) {
				((List) en.getValue()).add(newValue); // add new values including inserted
				// System.out.println("adding value after: " + en.toString());
			}
		}
		this.hierarchy.remove(keyToRemove);

		// System.out.println("removing entry: " + keyToRemove);
		// }
		//
		// for (Map.Entry en : this.hierarchy.entrySet()) {
		// List<String> key = (List) en.getKey();
		//
		// }
		logger.debug("merged --------------");
		for (Entry e : this.getPrimitiveHierarchy().entrySet()) {
			List<String> keys = (List<String>) e.getKey();
			List<String> values = (List<String>) e.getValue();
			for (String value : values)
				if (!this.getEpById(keys.get(0)).getOperatorIds().contains(value.split("\\|")[2]))
					logger.error("ERH inconsistency found.");
			logger.debug("ERH merged: " + this.toSimpleEntryString(e) + "\n");
		}
		return true;
	}

	private EventPattern getCandidateById(String id) {
		for (EventPattern ep : this.candidates)
			if (ep.getID().equals(id))
				return ep;
		return null;
	}

	public void printERH() {
		for (Entry e : this.hierarchy.entrySet()) {
			List<String> heads = (List<String>) e.getKey();
			int epSize = this.getEpById(heads.get(0)).getSize();
			List<String> childs = (List<String>) e.getValue();
			int childSize = childs.size();
			logger.info("ERH " + epSize + " " + childSize + ": " + this.toSimpleEntryString(e));
		}
	}

	@SuppressWarnings("rawtypes")
	private void removeEdge(String parent, String child, String code) throws Exception {

		if (parent == null || child == null) {
			// List<String> entryToRemove = null;
			// for (Map.Entry en : this.hierarchy.entrySet()) {
			// if (((List) en.getKey()).contains(child)) {
			// entryToRemove = (List) en.getKey();
			// break;
			// }
			// }
			// if (entryToRemove != null)
			// this.hierarchy.remove(entryToRemove);
			// System.out.println("Should not happen!");

			// } else if (child == null) {
			// List<String> entryToRemove = null;
			// for (Map.Entry en : this.hierarchy.entrySet()) {
			// if (((List) en.getKey()).contains(child)) {
			// entryToRemove = (List) en.getKey();
			// break;
			// }
			// }
			// if (entryToRemove != null)
			// this.hierarchy.remove(entryToRemove);
		} else {
			// logger.info("======================================");
			if (critical)
				logger.info("Removing edge: " + this.getEpById(parent).toSimpleString() + " "
						+ this.getEpById(child).toSimpleString());
			for (Map.Entry en : this.hierarchy.entrySet()) {
				if (((List) en.getKey()).contains(parent)) {
					String valueToRemove = null;
					for (String value : ((List<String>) en.getValue())) {
						if (Arrays.asList(value.split("\\|")[0].split(",")).contains(child)) {
							if (critical)
								logger.info("Removing b4: " + this.toSimpleEntryString(en));
							valueToRemove = value;
						}
					}
					if (valueToRemove != null) {
						((List) en.getValue()).remove(valueToRemove);
						if (critical)
							logger.info("Removed after: " + this.toSimpleEntryString(en));
					}
				}
			}
			if (this.hasEdge(parent, child))
				logger.error("Still has edge after removal.");
		}
	}

	public void removeFromHierarchy(EventPattern ep) throws Exception {
		// TODO add remove function
	}

	private void reusabilityTraverse(EventPattern ep, String root, String parent, ArrayList<String> visited)
			throws Exception {
		if (!visited.contains(root)) {
			// visited.add(root);
			if (critical)
				logger.info("Visiting: " + this.getEpById(root).toSimpleString());
			EventPattern rootEp = this.getEpById(root);
			// System.out.println("Comparing: ==============\n root: " + rootEp + "\n and: " + ep);
			String reusability = Comparator.reusable(rootEp, ep);
			if (critical)
				logger.info("Reusability: " + reusability);
			String code = reusability.split("\\|")[0];
			if (code.equals("nr") || code.equals("-nr")) { // not reusable

			} else if (code.equals("sub")) { // substitute
				// this.removeEdge(parent, ep.getID());
				this.mergeIdenticalPatterns(root, ep.getID());
				visited.addAll(this.getDescendants(root));
				// if (!merged.contains(ep.getID()))
				// merged.add(ep.getID());
				// return true;
			} else if (code.equals("dr") || code.equals("ir")) { // directly reusable
				// reused = true;
				if (critical)
					logger.info("removing edge caused by dr & ir .");
				this.removeEdge(parent, ep.getID(), code);
				this.addEdge(root, ep.getID(), reusability);
				List<String> childNodes = this.getChildNodes(root);
				for (String child : childNodes) {
					this.reusabilityTraverse(ep, child, root, visited);
				}
			} else if (code.equals("-dr") || code.equals("-ir")) {
				// visited.addAll(this.getDescendants(root));
				if (critical)
					logger.info("removing edge caused by -dr & -ir .");
				this.removeEdge(parent, root, code);
				this.addEdge(ep.getID(), root, reusability.substring(1));
			}
		}
		boolean hasError = false;
		for (Entry e : this.getPrimitiveHierarchy().entrySet()) {
			List<String> keys = (List<String>) e.getKey();
			List<String> values = (List<String>) e.getValue();
			for (String value : values)
				if (!this.getEpById(keys.get(0)).getOperatorIds().contains(value.split("\\|")[2])) {
					logger.error("ERH inconsistency found after visiting: " + root);
					logger.error("keys: " + keys);
					logger.error("value: " + values);
					hasError = true;
					break;
				}
			// logger.debug("ERH after: " + toSimpleEntryString(e) + "\n");
		}
		if (!hasError)
			logger.debug("Visited with sucess: " + this.getEpById(root).toSimpleString());
	}

	private void reversedReusabilityTraverse(EventPattern ep, String leaf, String child, ArrayList<String> visited)
			throws Exception {
		if (!visited.contains(leaf)) {
			// visited.add(leaf);
			EventPattern leafEp = this.getEpById(leaf);
			logger.debug("Reversed visiting: " + leafEp.toSimpleString());
			// System.out.println("Comparing: ==============\n root: " + rootEp + "\n and: " + ep);
			String reusability = Comparator.reusable(leafEp, ep);
			// System.out.println("result: " + reusability);
			String code = reusability.split("\\|")[0];
			if (code.equals("nr") || code.equals("-nr")) { // not reusable

			} else if (code.equals("sub")) { // substitute, should not happen in reversed traversal
				System.out.println("sub should not happen!");
				// this.removeEdge(ep.getID(), child);
				// this.mergeIdenticalPatterns(leaf, ep.getID());
			} else if (code.equals("dr") || code.equals("ir")) { // directly reusable
				// reused = true;\System.out.println("result: " + reusability);
				System.out.println("dr should not happen!");
				// this.removeEdge(leaf, child);
				// this.addEdge(leaf, ep.getID(), reusability, false);

			} else if (code.equals("-dr") || code.equals("-ir")) {
				this.removeEdge(ep.getID(), child, code);
				this.addEdge(ep.getID(), leaf, reusability.substring(1));
				List<String> parentNodes = this.getParentNodes(leaf);
				for (String parent : parentNodes) {
					this.reversedReusabilityTraverse(ep, parent, leaf, visited);
				}
			}
			boolean hasError = false;
			for (Entry e : this.getPrimitiveHierarchy().entrySet()) {
				List<String> keys = (List<String>) e.getKey();
				List<String> values = (List<String>) e.getValue();
				for (String value : values)
					if (!this.getEpById(keys.get(0)).getOperatorIds().contains(value.split("\\|")[2])) {
						logger.error("ERH inconsistency found after r visiting: " + leaf);
						logger.error("keys: " + keys);
						logger.error("value: " + values);
						hasError = true;

						System.exit(1);
					}
				// logger.debug("ERH after: " + toSimpleEntryString(e) + "\n");
			}
			if (!hasError)
				logger.debug("R-visited with sucess: " + this.getEpById(leaf).toSimpleString());
		}

	}

	public void setCandidates(List<EventPattern> candidates) {
		this.candidates = candidates;
	}

	// public void setHierarchy(Map<List<String>, List<String>> hierarchy) {
	// this.hierarchy = hierarchy;
	// }

	public void setIdentical(List<List<String>> identical) {
		this.identical = identical;
	}

	public void setQuery(List<EventPattern> query) {
		this.queries = query;
	}

	// private EventPattern getSubstituteWithMinimalFreq(EventPattern query) throws CloneNotSupportedException,
	// NodeRemovalException {
	// Double freq = -1.0;
	// EventPattern result = null;
	// for (EventPattern ep : this.getSubstitutes(query)) {
	// if (freq < 0 || ep.getFrequency() < freq) {
	// freq = ep.getFrequency();
	// result = ep;
	// }
	// }
	// return result; // null result if no match
	// }
	public String toSimpleEntryString(Entry e) {
		// String result = e.toString();
		// String original = e.toString();
		List<String> keys = (List<String>) e.getKey();
		List<String> values = (List<String>) e.getValue();
		Map<String, String> replacementMap = new HashMap<String, String>();
		for (String key : keys) {
			if (!replacementMap.containsKey(key))
				replacementMap.put(key, this.getEpById(key).toSimpleString());
		}
		List<String> newValues = new ArrayList<String>();
		for (String value : values) {
			List<String> valueIds = Arrays.asList(value.split("\\|")[0].split(","));
			for (String id : valueIds) {
				if (!replacementMap.containsKey(id))
					replacementMap.put(id, this.getEpById(id).toSimpleString());
			}
			newValues.add("[" + value.split("\\|")[0] + "]");
		}
		// values.removeAll(values);
		// values.addAll(newValues);
		String result = e.toString();
		for (Entry en : replacementMap.entrySet()) {
			result = result.replaceAll((String) en.getKey(), (String) en.getValue());
		}
		// for (Entry en : replacementMap.entrySet()) {
		// result = result.replaceAll((String) en.getValue() + "-0", "root");
		// }
		return result;
	}
}
