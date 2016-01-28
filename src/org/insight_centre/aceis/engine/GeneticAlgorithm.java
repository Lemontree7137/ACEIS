package org.insight_centre.aceis.engine;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;

import org.insight_centre.aceis.eventmodel.EventDeclaration;
import org.insight_centre.aceis.eventmodel.EventOperator;
import org.insight_centre.aceis.eventmodel.EventPattern;
import org.insight_centre.aceis.eventmodel.NodeRemovalException;
import org.insight_centre.aceis.eventmodel.QosVector;
import org.insight_centre.aceis.eventmodel.WeightVector;
import org.insight_centre.aceis.utils.MapUtils;
import org.insight_centre.aceis.utils.test.CEScore;
import org.insight_centre.aceis.utils.test.Simulator2;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * @author feng
 * 
 *         Genetic algorithm based qos composition implementation
 * 
 */
public class GeneticAlgorithm {

	// private
	public static final Logger logger = LoggerFactory.getLogger(GeneticAlgorithm.class);
	private Double bestFitness;
	private QosVector constraint;
	private CompositionPlanEnumerator cpe;
	private Double crossoverRate, mutationRate;
	private boolean fixedPopulationSize;
	private int populationSize, numberOfIteration;
	private Double selectionFactor;
	private int selectionMode;
	private WeightVector weight;

	public GeneticAlgorithm(CompositionPlanEnumerator cpe, int selectionMode, int populationSize,
			int numberOfIteration, Double crossoverRate, Double mutationRate, Double factor,
			boolean fixedPopulationSize, WeightVector weight, QosVector constraint) throws CloneNotSupportedException,
			NodeRemovalException {
		super();
		this.cpe = cpe;
		this.populationSize = populationSize;
		this.numberOfIteration = numberOfIteration;
		this.mutationRate = mutationRate;
		this.selectionFactor = factor;
		this.selectionMode = selectionMode;
		this.crossoverRate = crossoverRate;
		this.setFixedPopulationSize(fixedPopulationSize);
		// this.bestFitness = this.calculateBestFitness();
		// this.bestFitness = bestFitness;
		this.weight = weight;
		this.constraint = constraint;
	}

	public List<EventPattern> createNextGen(List<EventPattern> initialPopulation) throws Exception {
		// System.out.println("Before GEN size: " + initialPopulation.size());
		// logger.info("fixedPopSize: " + this.fixedPopulationSize);
		List<EventPattern> chosenOnes = this.selection(initialPopulation);
		// System.out.println("chosen: " + chosenOnes.size());
		List<EventPattern> nextGen = new ArrayList<EventPattern>();
		// immortal elite
		EventPattern elite = chosenOnes.get(chosenOnes.size() - 1);

		// chosenOnes.remove(elite);
		List<EventPattern> offSpring = this.crossover(chosenOnes);
		// for (EventPattern ep : chosenOnes)
		// System.out.println("Crossover Results: " + ep.toString());
		this.mutation(offSpring);
		nextGen.addAll(offSpring);
		nextGen.add(elite.clone().variateIds());
		// System.out.print(" Before GEN size: " + initialPopulation.size() + " Chosen: " + nextGen.size() + "\n");
		return nextGen;

	}

	/**
	 * @param ccps
	 * @return
	 * 
	 *         randomly pairs the CCPs for crossover
	 */
	private Map<String, String> createPairs(List<EventPattern> ccps) {
		Map<String, String> results = new HashMap<String, String>();
		List<String> paired = new ArrayList<String>();
		for (EventPattern ccp : ccps) {
			paired.add(ccp.getID());
		}
		while (paired.size() > 1) {
			int index1 = (int) (Math.random() * paired.size());
			String firstId = paired.get(index1);
			paired.remove(index1);
			int index2 = (int) (Math.random() * paired.size());
			String secondId = paired.get(index2);
			paired.remove(index2);
			results.put(firstId, secondId);
		}
		return results;
	}

	/**
	 * @param ccps
	 * @return ccps after cross over
	 * @throws Exception
	 * 
	 *             cross over the genes
	 */
	private List<EventPattern> crossover(List<EventPattern> ccps) throws Exception {
		// System.out.println("Crossing: ");
		List<EventPattern> results = new ArrayList<EventPattern>();
		Map<String, List<String>> encodings = this.encode(ccps);
		// System.out.println("Encodings: " + encodings);
		Map<String, String> pairs = this.createPairs(ccps);
		// System.out.println("pairs: " + pairs);
		for (Entry<String, String> e : pairs.entrySet()) {
			try {
				String firstEpId = e.getKey();
				String secondEpId = e.getValue();
				EventPattern firstEp = new EventPattern();
				EventPattern secondEp = new EventPattern();
				for (EventPattern ep : ccps) {
					if (ep.getID().equals(firstEpId))
						firstEp = ep;
					else if (ep.getID().equals(secondEpId))
						secondEp = ep;
				}
				if (Math.random() < this.crossoverRate) {
					results.addAll(this.swap(firstEp, secondEp, encodings, ccps));
				} else {
					// logger.info("Skipping crossover.");
					results.add(firstEp.clone().variateIds());
					results.add(secondEp.clone().variateIds());
				}

			} catch (Exception ex) {
				ex.printStackTrace();
				logger.error("SWAPPING ERROR: " + e.getKey() + ", " + e.getValue());
			}
		}
		return results;
	}

	/**
	 * @param ccps
	 * @return
	 * 
	 *         creates genetic encodings for event composition plans
	 */
	private Map<String, List<String>> encode(List<EventPattern> ccps) {
		Map<String, List<String>> results = new HashMap<String, List<String>>();
		for (EventPattern ep : ccps) {
			results.put(ep.getID(), new ArrayList<String>());
			for (EventDeclaration ed : ep.getEds()) {
				String idStr = "";
				String typeStr = "";
				String currentID = ed.getnodeId();
				idStr = currentID;
				if (ed.getEp() != null)
					typeStr = "complex";
				else
					typeStr = ed.getEventType();
				while (ep.getParent(currentID) != null) {
					idStr = ep.getParent(currentID) + ";" + idStr;
					typeStr = ((EventOperator) ep.getNodeById(ep.getParent(currentID))).getOpt() + "-" + typeStr;
					currentID = ep.getParent(currentID);
				}
				results.get(ep.getID()).add(idStr + "|" + typeStr);
			}
		}

		return results;
	}

	public List<EventPattern> evolution() throws Exception {
		logger.info("Starting Genetic Algorithm. " + "Population size: " + this.populationSize + ", crossover rate: "
				+ this.crossoverRate + ", mutation rate: " + this.mutationRate);
		long t1 = System.currentTimeMillis();
		List<EventPattern> population = this.populationInitialization();
		long t3 = System.currentTimeMillis();
		List<Double> avgUtilities = new ArrayList<Double>();
		Simulator2.utilityInGenerations.add(avgUtilities);
		int i = 1;
		double initAvgUtility = this.getAvgResult(population);
		logger.info("Initial best utility: " + this.getBestResult(population) + ", initial avg utility: "
				+ initAvgUtility);
		EventPattern initBestEp = this.getBestEP(population);
		logger.info("Initial best ep: " + initBestEp.toSimpleString() + ", qos: " + initBestEp.aggregateQos());
		avgUtilities.add(initAvgUtility);
		double convergenceAvgUtility = 0.0, convergenceMaxUtility = 0.0;
		// EventPattern result = new EventPattern();
		while (i < this.numberOfIteration && population.size() > 1) {
			logger.debug("Iteration: " + i);

			population = this.createNextGen(population);
			// double avgUtility = this.getAvgResult(population);
			// logger.info(" Avg Utility: " + avgUtility);

			String resultStr = this.getResult(population);
			double avgUtility = Double.parseDouble(resultStr.split("\\|")[0]);
			double bestUtility = Double.parseDouble(resultStr.split("\\|")[1]);
			convergenceAvgUtility = avgUtility;
			convergenceMaxUtility = bestUtility;
			avgUtilities.add(avgUtility);
			if (((bestUtility - avgUtility) / initAvgUtility) <= 0.05 || population.size() <= 6)
				break;
			i += 1;
		}
		long t2 = System.currentTimeMillis();
		if (Simulator2.CEScores.size() > 0) {
			CEScore ces = new CEScore(t2 - t1, t3 - t1, initAvgUtility, convergenceAvgUtility, convergenceMaxUtility);
			Simulator2.CEScores.get(Simulator2.CEScores.size() - 1).add(ces);
		}
		logger.info("Converged best utility: " + convergenceMaxUtility + ", converged avg utility: "
				+ convergenceAvgUtility + ", within: " + (t2 - t1) + " ms.");
		return population;// add result output
	}

	private Double getAvgResult(List<EventPattern> population) throws Exception {
		Double sum = 0.0;
		for (EventPattern ep : population) {
			sum += Comparator.calculateUtility(constraint, weight, ep.aggregateQos());
		}
		return sum / population.size();
	}

	public Double getBestFitness() {
		return bestFitness;
	}

	private Double getBestResult(List<EventPattern> population) throws Exception {
		Double bestFitness = -600000.0;
		// EventPattern best = new EventPattern();
		for (EventPattern ccp : population) {
			QosVector capability = ccp.aggregateQos();
			if (capability.satisfyConstraint(constraint)) {
				Double ccpFitness = Comparator.calculateUtility(constraint, weight, ccp.aggregateQos());
				if (ccpFitness > bestFitness) {
					bestFitness = ccpFitness;
					// best = ccp;
				}
			}
		}
		// System.out.println("Best EP:" + best.toString());
		// System.out.println("Best Qos:" + best.aggregateQos());
		// System.out.println("Best Utility:" + bestFitness);
		return bestFitness;

	}

	public EventPattern getBestEP(List<EventPattern> population) throws Exception {
		Double bestFitness = -600000.0;
		EventPattern best = new EventPattern();
		for (EventPattern ccp : population) {
			QosVector capability = ccp.aggregateQos();
			if (capability.satisfyConstraint(constraint)) {
				Double ccpFitness = Comparator.calculateUtility(constraint, weight, ccp.aggregateQos());
				if (ccpFitness > bestFitness) {
					bestFitness = ccpFitness;
					best = ccp;
				}
			}
		}
		// System.out.println("Best EP:" + best.toString());
		// System.out.println("Best Qos:" + best.aggregateQos());
		// System.out.println("Best Utility:" + bestFitness);
		return best;

	}

	public QosVector getConstraint() {
		return constraint;
	}

	public Double getMutationRate() {
		return mutationRate;
	}

	public int getNumberOfIteration() {
		return numberOfIteration;
	}

	public int getPopulationSize() {
		return populationSize;
	}

	/**
	 * @param firstEp
	 * @param secondEp
	 * @param encodings
	 * @param ccps
	 * @return a set of paired replaceable node ids
	 * @throws Exception
	 * 
	 *             finds randomly a pair of replaceable nodes (crossover points)
	 */
	private String getReplaceableNodes(EventPattern firstEp, EventPattern secondEp,
			Map<String, List<String>> encodings, List<EventPattern> ccps) throws Exception {
		List<String> encodings1 = encodings.get(firstEp.getID());
		List<String> encodings2 = encodings.get(secondEp.getID());
		if (encodings1.size() == 1 || encodings2.size() == 1)
			return null;
		Set<String> visitied = new HashSet<String>();

		while (visitied.size() < encodings1.size()) {
			int index1 = (int) (Math.random() * encodings1.size());
			String chosenStr1 = encodings1.get(index1);
			String idStr1 = chosenStr1.split("\\|")[0];
			String typeStr1 = chosenStr1.split("\\|")[1];
			String edId1 = idStr1.split(";")[idStr1.split(";").length - 1];
			// EventDeclaration ed1 = (EventDeclaration) firstEp.getNodeById(edId1);
			String edType1 = typeStr1.split("-")[typeStr1.split("-").length - 1];
			// System.out.println("IdStr1: " + idStr1 + " TyoeStr1: " + typeStr1 + " edid1: " + edId1);

			if (!visitied.contains(chosenStr1)) {
				visitied.add(chosenStr1);
				for (String s : encodings2) {
					String idStr2 = s.split("\\|")[0];
					String typeStr2 = s.split("\\|")[1];
					String edId2 = idStr2.split(";")[idStr2.split(";").length - 1];
					// EventDeclaration ed2 = (EventDeclaration) secondEp.getNodeById(edId2);
					String edType2 = typeStr2.split("-")[typeStr2.split("-").length - 1];
					// System.out.println("IdStr2: " + idStr2 + " TyoeStr2: " + typeStr2 + " edid2: " + edId2);
					String result = "";
					if (typeStr1.equals(typeStr2)) {
						// if (typeStr1.equals(typeStr2) && !ed1.getServiceId().equals(ed2.getServiceId())) {
						if (Comparator.isCanonicalSubstitute(firstEp.getSubtreeByRoot(edId1),
								secondEp.getSubtreeByRoot(edId2))) {

							result = edId1 + ";" + edId2;
						}
					} else if (typeStr1.substring(0, typeStr1.length() - edType1.length()).contains(
							typeStr2.substring(0, typeStr2.length() - edType2.length()))) {
						if (Comparator.isCanonicalSubstitute(
								firstEp.getSubtreeByRoot(idStr1.split(";")[idStr2.split(";").length - 1]),
								secondEp.getSubtreeByRoot(edId2))) {
							result = idStr1.split(";")[idStr2.split(";").length - 1] + ";" + edId2;
						}
					} else if (typeStr2.substring(0, typeStr2.length() - edType2.length()).contains(
							typeStr1.substring(0, typeStr1.length() - edType1.length()))) {
						if (Comparator.isCanonicalSubstitute(
								secondEp.getSubtreeByRoot(idStr2.split(";")[idStr1.split(";").length - 1]),
								firstEp.getSubtreeByRoot(edId1))) {
							result = edId1 + ";" + idStr2.split(";")[idStr1.split(";").length - 1];
						}
					}
					if (!result.equals("")) {
						// EventDeclaration ed3 = (EventDeclaration) firstEp.getNodeById(result.split(";")[0]);
						// EventDeclaration ed4 = (EventDeclaration) secondEp.getNodeById(result.split(";")[1]);
						// // logger.info("Replaceable: " + result);
						// if (!ed3.getServiceId().equals(ed4.getServiceId()))
						// return result;
						Object node1 = firstEp.getNodeById(result.split(";")[0]);
						Object node2 = secondEp.getNodeById(result.split(";")[1]);
						if (node1 instanceof EventDeclaration && node2 instanceof EventDeclaration) {
							EventDeclaration ed3 = (EventDeclaration) node1;
							EventDeclaration ed4 = (EventDeclaration) node2;
							// logger.info("Replaceable: " + result);
							if (!ed3.getServiceId().equals(ed4.getServiceId()))
								return result;
						} else
							return result;
					}
				}
			}
		}
		// when identical patterns return null
		return null;
	}

	private String getResult(List<EventPattern> population) throws Exception {
		Double best = -100.0;
		Double sum = 0.0;
		// Double sum = 0.0;
		for (EventPattern ep : population) {
			Double u = Comparator.calculateUtility(constraint, weight, ep.aggregateQos());
			sum += u;
			if (best <= u)
				best = u;
		}
		logger.debug("Best: " + best + " , Avg: " + sum / population.size() + ", Pop size: " + population.size());
		return sum / population.size() + " | " + best;
	}

	public WeightVector getWeight() {
		return weight;
	}

	/**
	 * @param nextGen
	 * @throws Exception
	 *             mutates the event patterns/composition plans
	 */
	private void mutation(List<EventPattern> nextGen) throws Exception {
		for (EventPattern ep : nextGen) {
			if (Math.random() <= this.mutationRate) {
				int index = (int) (Math.random() * ep.getEds().size());
				EventDeclaration mutated = ep.getEds().get(index);
				// logger.info("Mutating: " + ep.toSimpleString() + " @" + mutated.getServiceId());
				// System.out.println("Chosen Node: " + mutated.toString());
				List<EventDeclaration> candidates = new ArrayList<EventDeclaration>();
				candidates.addAll(this.cpe.getRepo().getEds().values());
				// for (EventDeclaration e : candidates)
				// if (e.getServiceId().equals(mutated.getServiceId())) {
				// candidates.remove(e);
				// break;
				// }
				// logger.info("candidates: " + candidates.size());
				if (mutated.getEp() == null) {
					EventDeclaration mutation = cpe.getRandomSubstituteEDforED(mutated, candidates);
					if (mutation != null)// TODO consider when mutation is null for primtiive events
						ep.replaceED(mutated.getnodeId(), mutation);
					else
						logger.warn("Replacement is null for: " + mutated.getServiceId());
				} else {
					List<EventPattern> mutationACPs = cpe.getACPsForQuery(mutated.getEp().getCanonicalPattern());
					int epIndex = 0;

					// List<EventDeclaration> candidates = new ArrayList<EventDeclaration>();
					EventDeclaration mutationEd = cpe.getRandomSubstituteEDforED(mutated, candidates);
					if (mutationEd == null)
						epIndex = (int) (Math.random() * mutationACPs.size());
					else
						epIndex = (int) (Math.random() * (mutationACPs.size() + 1));
					EventPattern mutationEp = null;
					if (epIndex != mutationACPs.size()) {
						mutationEp = cpe.getRandomCCPforACP(mutationACPs.get(epIndex), candidates);
						ep.replaceSubtree(mutated.getnodeId(), mutationEp);
						ep = ep.getReducedPattern();
					} else
						ep.replaceED(mutated.getnodeId(), mutationEd);

				}
				// logger.info("Mutated: " + ep.toSimpleString());
			}
		}

	}

	/**
	 * @return inital population
	 * @throws Exception
	 * 
	 *             initializes the population for evolution
	 */
	private List<EventPattern> populationInitialization() throws Exception {
		long t1 = System.currentTimeMillis();
		List<EventPattern> acps = cpe.getACPs();
		long t3 = System.currentTimeMillis();
		// RDFFileManager.writeEPsToFile("resultlog/examples/ACPs.n3", acps);
		// for (EventPattern ep : acps)
		// System.out.println("acp:" + ep);
		List<EventPattern> ccps = new ArrayList<EventPattern>();
		this.variation(acps, ccps);
		// for (EventPattern ccp : ccps)
		// logger.info("Initial Population: " + ccp + "\n");

		long t2 = System.currentTimeMillis();
		logger.info("Initialization Time: " + (t2 - t1) + ", ACP creation Time: " + (t3 - t1));
		return ccps;
	}

	private void printResults(Double avg, Double best) {
		System.out.println(avg);
	}

	/**
	 * @param initialPopulation
	 * @return selected population
	 * @throws Exception
	 */
	private List<EventPattern> selection(List<EventPattern> initialPopulation) throws Exception {
		List<EventPattern> results = new ArrayList<EventPattern>();
		Double maxUtility = -60000.0;
		Double minUtility = 60000.0;
		Double sumUtility = 0.0;
		int size = initialPopulation.size();
		Map<EventPattern, Double> utilityMap = new HashMap<EventPattern, Double>();
		for (EventPattern ep : initialPopulation) {
			// logger.info("Calculating: " + ep.toSimpleString() + "\n QoS:" + ep.aggregateQos());
			Double utility = Comparator.calculateUtility(constraint, weight, ep.aggregateQos());
			if (maxUtility <= utility)
				maxUtility = utility;
			if (minUtility >= utility)
				minUtility = utility;
			utilityMap.put(ep, utility);
			sumUtility += utility;
		}
		utilityMap = MapUtils.sortByValue(utilityMap);
		if (!this.isFixedPopulationSize()) {
			// logger.info("selecting: ");
			if (this.selectionMode == 0) {
				for (Entry<EventPattern, Double> e : utilityMap.entrySet()) {
					if (e.getValue() < 0)
						logger.warn("utility < 0");
					if (Math.random() <= ((e.getValue()) / (maxUtility)) + this.selectionFactor) // for -3<=u<=3
						results.add(e.getKey());
				}
			} else {
				int cnt = 1;
				for (Entry<EventPattern, Double> e : utilityMap.entrySet()) {
					if (Math.random() <= ((cnt + 0.0) / size) + this.selectionFactor)
						results.add(e.getKey());
					cnt += 1;
				}
			}
		} else {
			for (int i = 0; i < initialPopulation.size(); i++) {
				Double r = Math.random() * sumUtility;
				// int cnt = 1;
				Double tempSum = 0.0;
				for (Entry<EventPattern, Double> e : utilityMap.entrySet()) {
					tempSum += e.getValue();
					if (r > tempSum)
						continue;
					else {
						results.add(e.getKey().clone().variateIds());
						break;
					}
					// cnt += 1;
				}
			}
		}
		// this.printResults(avgUtility / initialPopulation.size(), maxUtility);
		return results;
	}

	public void setBestFitness(Double bestFitness) {
		this.bestFitness = bestFitness;
	}

	public void setConstraint(QosVector constraint) {
		this.constraint = constraint;
	}

	public void setMutationRate(Double mutationRate) {
		this.mutationRate = mutationRate;
	}

	public void setNumberOfIteration(int numberOfIteration) {
		this.numberOfIteration = numberOfIteration;
	}

	public void setPopulationSize(int populationSize) {
		this.populationSize = populationSize;
	}

	public void setWeight(WeightVector weight) {
		this.weight = weight;
	}

	/**
	 * @param e
	 * @param encodings
	 * @param ccps
	 * @return patterns after swapping genes
	 * @throws Exception
	 * 
	 *             exchanges genes according to the chosen crossover point
	 */
	private List<EventPattern> swap(EventPattern firstEp, EventPattern secondEp, Map<String, List<String>> encodings,
			List<EventPattern> ccps) throws Exception {
		// String firstEpId = e.getKey();
		// String secondEpId = e.getValue();
		// EventPattern firstEp = new EventPattern();
		// EventPattern secondEp = new EventPattern();
		// for (EventPattern ep : ccps) {
		// if (ep.getID().equals(firstEpId))
		// firstEp = ep;
		// else if (ep.getID().equals(secondEpId))
		// secondEp = ep;
		// }
		// String firstRoot = firstEp.getRootId();
		// String secondRoot = secondEp.getRootId();
		// System.out.println("Swapping ep1: " + firstEp + "\n Qos: " + firstEp.aggregateQos());
		// System.out.println("Swapping ep2: " + secondEp + "\n Qos: " + secondEp.aggregateQos());
		// Set<String> eids = new HashSet<String>();
		// for (EventDeclaration ed : firstEp.getEds())
		// eids.add(ed.getServiceId());
		String replaceables = this.getReplaceableNodes(firstEp, secondEp, encodings, ccps);
		// System.out.println("Replaceables: " + replaceables);
		boolean identical = true;
		if (replaceables != null) {// if identical, keep in generation
			identical = false;
			String node1 = replaceables.split(";")[0];
			String node2 = replaceables.split(";")[1];
			// System.out.println("node1: " + node1);
			// System.out.println("node2: " + node2);
			EventPattern part1 = firstEp.getSubtreeByRoot(node1).clone();
			EventPattern part2 = secondEp.getSubtreeByRoot(node2).clone();
			// System.out.println("Swapping part1: " + part1);
			// System.out.println("Swapping part2: " + part2);
			firstEp.replaceSubtree(node1, part2);// may need renaming nodes
			secondEp.replaceSubtree(node2, part1);
		}
		// Set<String> eids2 = new HashSet<String>();
		// for (EventDeclaration ed : firstEp.getEds())
		// eids2.add(ed.getServiceId());
		// if (eids.containsAll(eids2) && eids2.containsAll(eids))
		// logger.warn("Crossover did not take effect. Due to identical pair: " + identical);
		// System.out.println("Swapped ep1: " + firstEp + "\n Qos: " + firstEp.aggregateQos());
		// System.out.println("Swapped ep2: " + secondEp + "\n Qos: " + secondEp.aggregateQos());
		List<EventPattern> results = new ArrayList<EventPattern>();
		results.add(firstEp.clone().variateIds());
		results.add(secondEp.clone().variateIds());
		// results.add(firstEp)
		return results;
	}

	/**
	 * @param acps
	 * @param ccps
	 * @throws Exception
	 * 
	 *             randomly chooses the ACPs as templates to create CCPs
	 */
	private void variation(List<EventPattern> acps, List<EventPattern> ccps) throws Exception {
		Map<String, List<String>> acpImpl = new HashMap<String, List<String>>();
		while (ccps.size() <= this.populationSize) {
			int index = (int) (Math.random() * acps.size());
			EventPattern template = acps.get(index);
			EventPattern ccp = template.clone();
			ccp.setID("CCP-" + ccps.size());
			if (acpImpl.get(template.getID()) == null)
				acpImpl.put(template.getID(), new ArrayList<String>());
			String replacementStr = "";
			for (EventDeclaration tempED : template.getEds()) {
				List<EventDeclaration> candidates = cpe.getAllSubsituteEDforED(tempED);
				if (Math.pow((candidates.size() + 0.0), (0.0 + template.getEds().size())) < this.populationSize + 0.0) {
					logger.error("Insufficient solution space. Use more candidates or switch to brute force.");
					System.exit(1);
				}
				int edIndex = (int) (Math.random() * candidates.size());
				ccp.replaceED(tempED.getnodeId(), candidates.get(edIndex));
				// make sure no dupliates are created in the initial population
				replacementStr += candidates.get(edIndex).getnodeId();
			}
			if (acpImpl.get(template.getID()).contains(replacementStr))
				continue;
			else
				acpImpl.get(template.getID()).add(replacementStr);
			ccp.setQuery(false);
			ccps.add(ccp);
		}
		// return null;
	}

	public boolean isFixedPopulationSize() {
		return fixedPopulationSize;
	}

	public boolean setFixedPopulationSize(boolean fixedPopulationSize) {
		this.fixedPopulationSize = fixedPopulationSize;
		return fixedPopulationSize;
	}

}
