package org.insight_centre.aceis.utils.test;

import java.io.File;
import java.io.FileOutputStream;
import java.io.FileWriter;
import java.io.IOException;
import java.io.OutputStream;
import java.net.URI;
import java.net.URISyntaxException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.CountDownLatch;

import org.deri.cqels.engine.ExecContext;
//import org.deri.cqels.websocket.client.ClientSubscriber;
import org.glassfish.tyrus.client.ClientManager;
//import org.insight.engine.ContextualFilteringManager;
import org.insight_centre.aceis.engine.ACEISEngine;
import org.insight_centre.aceis.engine.ACEISEngine.RspEngine;
import org.insight_centre.aceis.engine.ACEISFactory;
import org.insight_centre.aceis.engine.Comparator;
import org.insight_centre.aceis.engine.CompositionPlanEnumerator;
import org.insight_centre.aceis.engine.GeneticAlgorithm;
import org.insight_centre.aceis.eventmodel.EventDeclaration;
import org.insight_centre.aceis.eventmodel.EventOperator;
import org.insight_centre.aceis.eventmodel.EventPattern;
import org.insight_centre.aceis.eventmodel.OperatorType;
import org.insight_centre.aceis.eventmodel.QosVector;
import org.insight_centre.aceis.eventmodel.WeightVector;
import org.insight_centre.aceis.io.EventRepository;
import org.insight_centre.aceis.io.rdf.RDFFileManager;
import org.insight_centre.aceis.io.rdf.TextFileManager;
import org.insight_centre.aceis.subscriptions.SubscriptionManager;
import org.insight_centre.aceis.subscriptions.SubscriptionManagerFactory;
import org.insight_centre.aceis.utils.Partitioner;
import org.insight_centre.aceis.utils.ServiceGenerator;
import org.insight_centre.citypulse.client.ClientSubscriber;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.csvreader.CsvWriter;
import com.hp.hpl.jena.rdf.model.Model;
import com.hp.hpl.jena.rdf.model.ModelFactory;
import com.hp.hpl.jena.rdf.model.RDFWriter;
import com.hp.hpl.jena.reasoner.ReasonerRegistry;

/**
 * @author feng
 * 
 *         Event service composition test class
 * 
 */
public class Simulator2 {
	public static List<List<CEScore>> CEScores = new ArrayList<List<CEScore>>();
	private static int idcnt = 0;
	public static File log;

	// public static FileWriter logger;
	public enum QosSimulationMode {
		none, latency, completeness, accuracy;
	}

	public static final Logger logger = LoggerFactory.getLogger(Simulator2.class);
	private final static String path = "patterns/";
	public static List<List<Double>> utilityInGenerations = new ArrayList<List<Double>>();
	static List<String> mutationParams = new ArrayList<String>(), crossoverParams = new ArrayList<String>(),
			populationParams = new ArrayList<String>();
	static {
		mutationParams.add("m=0.0");
		mutationParams.add("m=0.01");
		mutationParams.add("m=0.02");
		mutationParams.add("m=0.03");
		mutationParams.add("m=0.04");
		mutationParams.add("m=0.05");
		mutationParams.add("m=0.06");
		mutationParams.add("m=0.07");
		mutationParams.add("m=0.08");
		mutationParams.add("m=0.09");
		mutationParams.add("m=0.1");
		mutationParams.add("m=0.2");
		mutationParams.add("m=0.5");

		// crossoverParams.add("c=0.0");
		// crossoverParams.add("c=0.05");
		crossoverParams.add("c=0.1");
		crossoverParams.add("c=0.15");
		crossoverParams.add("c=0.2");
		crossoverParams.add("c=0.25");
		crossoverParams.add("c=0.3");
		crossoverParams.add("c=0.35");
		crossoverParams.add("c=0.4");
		crossoverParams.add("c=0.45");
		crossoverParams.add("c=0.5");
		crossoverParams.add("c=0.55");
		crossoverParams.add("c=0.6");
		crossoverParams.add("c=0.65");
		crossoverParams.add("c=0.70");
		crossoverParams.add("c=0.75");
		crossoverParams.add("c=0.80");
		crossoverParams.add("c=0.85");
		crossoverParams.add("c=0.90");
		crossoverParams.add("c=0.95");
		crossoverParams.add("c=1.0");

		// populationParams.add("p=20");
		populationParams.add("p=40");
		populationParams.add("p=60");
		populationParams.add("p=80");
		populationParams.add("p=100");
		populationParams.add("p=120");
		populationParams.add("p=140");
		populationParams.add("p=160");
		populationParams.add("p=180");
		populationParams.add("p=200");
		populationParams.add("p=300");
		populationParams.add("p=500");
	}

	public static void main(String[] args) {

		Simulator2 sim = new Simulator2();
		// sim.initialize(3);
		try {
			int mode = 1;
			System.out.println("========Initialization Begin (preprocessing), Test Mode: " + mode + "========");
			String paramSetting = "pf=100";
			// System.out.println("Test Mode: " + mode);
			// ExecContext context = RDFFileManager.initializeContext("jws/SimRepo-3.n3",
			// ReasonerRegistry.getRDFSReasoner());
			// EventRepository er = RDFFileManager.buildRepoFromFile(0);
			// ACEISEngine.initialize(Mode.CSPARQL, "jws/SimRepo-8.n3");
			ACEISEngine engine = ACEISFactory.createACEISInstance(RspEngine.CSPARQL, null);
			engine.initialize("toitRepo/simrepo-9-10.n3");
			// ACEISEngine.initialize(Mode.CSPARQL, "toitRepo/simrepo-50.n3");
			// EventPattern query = RDFFileManager.extractQueryFromDataset("../resultlog/examples/EventRequest-10.n3");
			// EventPattern query = RDFFileManager.extractQueryFromDataset("../resultlog/examples/EventRequest.n3");
			EventPattern query = ServiceGenerator.createBaseEvent(7);
			// EventPattern query = null;
			// mutationParams.add("0.01");
			sim.repo = engine.getRepo();
			sim.query = query;
			// if (mode == 5) {
			// EventPattern compositionPlan = RDFFileManager.extractCompositionPlanFromDataset("");
			// // sim.testQT(compositionPlan);
			// System.exit(0);
			// }
			// System.exit(0);
			long t = 10;
			// if(!mode==4)
			// mutationParams, crossoverParams, populationParams
			if (mode == 4) {
				for (String param : mutationParams)
					sim.testGA(sim.repo, 3, t, param);
				sim.writeCEScores("M-Q1-R9'", mutationParams);
				System.exit(0);
			}
			EventPattern plan = sim.testGA(sim.repo, mode, t, paramSetting);

			// sim.writeAvgUtilities(paramSetting, t);

			// EventDeclaration compositionPlan = new EventDeclaration("EC" + "-" + plan.getID(), "EC" + "-"
			// + plan.getID(), "complex", plan, null, null);
			// compositionPlan.setComposedFor(sim.query);
			// compositionPlan.setServiceId(RDFFileManager.defaultPrefix + "CES" + "-" + UUID.randomUUID());
			logger.info("Composition Plan: " + plan.toSimpleString());
			logger.info("QoS: " + plan.aggregateQos() + "锛�Utility: ");
			// sim.testQT(compositionPlan);
			// startClient();
		} catch (Exception e) {
			e.printStackTrace();
		}
	}

	private Double crossoverRate = 1.0;

	private Double factor = 0.0;

	private boolean fixedPopulationSize = false;

	private int iterations = 1000;
	private Map<String, Integer> messageCnt = new HashMap<String, Integer>();
	private Double mutationRate = 0.03;
	private int populationSize = 300;
	private EventPattern query;
	private EventRepository repo;
	private int selectionMode = 0;
	private int testIt = 30;

	public Simulator2() {
		this.repo = new EventRepository();
	}

	private void buildRepositoryFromFile(String filePath, String file2) throws IOException {
		this.repo = TextFileManager.buildRepoFromFile(filePath, file2);
	}

	private List<EventDeclaration> createMultipleRandomEDforEP(EventPattern ep, int cardinality) throws Exception {
		List<EventDeclaration> results = new ArrayList<EventDeclaration>();
		// cardinality = ep.getSize() * cardinality;
		for (int i = 0; i < cardinality + 1; i++) {
			results.add(this.createRandomEDforEP(ep));
		}
		return results;
	}

	@SuppressWarnings("rawtypes")
	private List<EventPattern> createQueryPartitions(EventPattern query) throws Exception {
		List<EventPattern> results = new ArrayList<EventPattern>();
		EventOperator root = (EventOperator) query.getNodeById(query.getRootId());
		String queryRoot = root.getID();
		OperatorType rootOpt = root.getOpt();

		ArrayList partitions = new ArrayList();
		if (rootOpt == OperatorType.seq)
			partitions = Partitioner.getOrderedPartition((ArrayList) query.getChildIds(queryRoot));
		else
			partitions = Partitioner.getUnorderedPartition((ArrayList) query.getChildIds(queryRoot));
		// System.out.println(partitions);
		// for (Object part : partitions)
		// System.out.println(part);
		List<String> parts = new ArrayList<String>();
		for (int i = 0; i < partitions.size(); i++) {
			Object partition = partitions.get(i);
			for (Object part : (ArrayList) partition) {
				if (parts.contains(part.toString()))
					continue;
				EventPattern tempTree = new EventPattern("temp" + "-sub", new ArrayList<EventDeclaration>(),
						new ArrayList<EventOperator>(), new HashMap<String, List<String>>(),
						new HashMap<String, String>(), 1, -1);
				List<EventPattern> tempSubTrees = new ArrayList<EventPattern>();
				for (Object s : (ArrayList) part) {
					tempSubTrees.add(query.getSubtreeByRoot((String) s));
				}
				if (tempSubTrees.size() > 1) {
					EventOperator tempRoot = new EventOperator(rootOpt, root.getCardinality(), "0");
					tempTree.getEos().add(tempRoot);
					tempTree.getProvenanceMap().put(tempRoot.getID(), new ArrayList<String>());
					tempTree.addDSTs(tempSubTrees, tempRoot);
					// System.out.println("temp tree: " + tempTree);
					tempTree.setQuery(true);
					results.add(tempTree.variateIds());
				}
				parts.add(part.toString());
			}
		}
		return results;
	}

	private EventDeclaration createRandomEDforEP(EventPattern ep) throws Exception {
		EventDeclaration ed;
		// Double freq = Math.random() * 3;
		QosVector qos = QosVector.getRandomQos();
		String id = RDFFileManager.defaultPrefix + UUID.randomUUID();
		if (ep.getSize() > 1) {
			CompositionPlanEnumerator cpe = new CompositionPlanEnumerator(this.repo, this.query);
			List<EventDeclaration> candidates = new ArrayList<EventDeclaration>();
			candidates.addAll(this.repo.getEds().values());
			EventPattern ccp = cpe.getRandomCCPforACP(ep, candidates);
			ed = new EventDeclaration(id, "", "complex", ccp, null, null, qos);
			ccp.setID("EP-" + UUID.randomUUID());
		} else {
			ed = new EventDeclaration(id, "", ep.getEds().get(0).getEventType(), null, null, null, qos);
			System.out.println("ED created with null EP.");
		}
		ed.setServiceId(id);
		return ed;
	}

	public Double getCrossoverRate() {
		return crossoverRate;
	}

	public Double getFactor() {
		return factor;
	}

	public int getIterations() {
		return iterations;
	}

	public Double getMutationRate() {
		return mutationRate;
	}

	public int getPopulationSize() {
		return populationSize;
	}

	public int getSelectionMode() {
		return selectionMode;
	}

	public int getTestIt() {
		return testIt;
	}

	private void initialize(int cardinality) throws Exception {
		ExecContext context = RDFFileManager.initializeCQELSContext("jws/SimRepo-3.n3",
				ReasonerRegistry.getRDFSReasoner());
		EventRepository er = RDFFileManager.buildRepoFromFile(0);
		// System.out.println(er.getEds().size());
		EventPattern query = RDFFileManager.extractQueryFromDataset("I2-event_request.n3");

		this.repo = er;
		this.query = query;

		List<EventPattern> eps = this.createQueryPartitions(query);

		List<EventDeclaration> newEds1 = new ArrayList<EventDeclaration>();
		for (EventDeclaration ed : er.getEds().values()) {
			ed.setInternalQos(QosVector.getRandomQos());
			ed.setFrequency(Math.random() * 3);
			// System.out.println("Original ed: " + ed);
			// if (ed instanceof TrafficReportService)
			// System.out.println("report id: " + ((TrafficReportService) ed).getReportId());
			for (int i = 0; i < cardinality; i++) {
				// TODO change source location?
				EventDeclaration newEd = ed.clone();
				String id = newEd.getnodeId();
				id += "clone" + i;
				newEd.setnodeId(id);
				newEd.setServiceId(id);
				newEd.setInternalQos(QosVector.getRandomQos());
				// System.out.println("new clone ed: " + newEd);
				// if (newEd instanceof TrafficReportService)
				// System.out.println("new report id: " + ((TrafficReportService) newEd).getReportId());
				newEds1.add(newEd);
			}
		}
		for (EventDeclaration ed : newEds1) {
			er.getEds().put(ed.getnodeId(), ed);
		}

		for (EventPattern ep : eps) {

			List<EventDeclaration> newEds2 = this.createMultipleRandomEDforEP(ep, cardinality);
			for (EventDeclaration ed : newEds2) {
				er.getEds().put(ed.getnodeId(), ed);
				// System.out.println("=============================");
				// System.out.println("Adding ED: " + ed);
				String epId = ed.getEp().getID();
				er.getEps().put(epId, ed.getEp());
				// System.out.println("Adding EP: " + ed.getEp());
			}
		}
		System.out.println("EPs size: " + eps.size() + " Repo size: " + repo.getEds().size());
		RDFFileManager.writeRepoToFile("simulatedRepo.n3", repo);
	}

	public boolean isFixedPopulationSize() {
		return fixedPopulationSize;
	}

	public void setCrossoverRate(Double crossoverRate) {
		this.crossoverRate = crossoverRate;
	}

	public void setFactor(Double factor) {
		this.factor = factor;
	}

	public void setFixedPopulationSize(boolean fixedPopulationSize) {
		this.fixedPopulationSize = fixedPopulationSize;
	}

	public void setIterations(int iterations) {
		this.iterations = iterations;
	}

	public void setMutationRate(Double mutationRate) {
		this.mutationRate = mutationRate;
	}

	public void setPopulationSize(int populationSize) {
		this.populationSize = populationSize;
	}

	public void setSelectionMode(int selectionMode) {
		this.selectionMode = selectionMode;
	}

	public void setTestIt(int testIt) {
		this.testIt = testIt;
	}

	private List<Double> sortAvgUtilities(List<List<Double>> uig) {
		List<Double> results = new ArrayList<Double>();
		int maxGen = 0;
		for (List testIt : uig) {
			if (maxGen < testIt.size())
				maxGen = testIt.size();
		}
		for (int i = 0; i < maxGen; i++) {
			double sum = 0.0;
			int validListCnt = 0;
			for (List<Double> testIt : uig) {
				if (testIt.size() > i) {
					sum += testIt.get(i);
					validListCnt += 1;
				}
			}
			results.add(sum / validListCnt);
		}
		return results;
	}

	private void startClient() throws URISyntaxException {
		URI uri = new URI("ws://127.0.0.1:1234/websockets/subscribe");

		CountDownLatch latch = new CountDownLatch(1);

		ClientManager client = ClientManager.createClient();
		try {
			client.connectToServer(ClientSubscriber.class, uri);// new
																// URI("ws://localhost:8080/websockets/subscribe"));
			latch.await();

		} catch (Exception e) {
			e.printStackTrace();
		}

	}

	private EventPattern testGA(EventRepository repo, int mode, long time, String param) throws Exception {
		// repo.getEps().put(query.getID(), query);
		// logger.info("inserting query: " + query.toString());
		// repo.getReusabilityHierarchy().insertIntoHierarchy(query);

		EventPattern result = null;
		CompositionPlanEnumerator cpe = new CompositionPlanEnumerator(repo, query);
		// List<EventPattern> acps = cpe.getACPsForQuery(query);
		WeightVector weight = new WeightVector(1.0, 1.0, 1.0, 1.0, 1.0, 1.0); // optimize latency
		// WeightVector weight = new WeightVector(0.01, 0.01, 0.01, 1.0, 0.01, 0.01); // optimize accuracy
		// WeightVector weight = new WeightVector(0.01, 0.01, 0.01, 0.01, 1.0, 0.01); // optimize completeness
		// WeightVector weight = new WeightVector(0.01, 0.01, 0.01, 0.01, 0.01, 1.0); // optimize traffic
		// WeightVector weight = new WeightVector(0.01, 1.0, 0.01, 0.01, 0.01, 0.01); // optimize latency
		logger.info("Weight Vector: " + weight.toString());
		QosVector constraint = new QosVector(3000, 2700, 1, 0.0, 0.0, 50.0);
		logger.info("Constraint Vector: " + constraint.toString());
		if (mode == 0) {
			System.out.println("========Brute-Force Algorithm Begin (run-time calculation option #1)========");
			repo.getEps().put(query.getID(), query);
			logger.info("inserting query: " + query.toString());
			repo.getReusabilityHierarchy().insertIntoHierarchy(query);
			result = cpe.calculateBestFitness(weight, constraint);
		} else if (mode == 1) {
			// int populationSize = 300;
			// int iterations = 100;
			// Double mutationRate = 0.5;
			// Double factor = 0.0;
			// int selectionMode = 0; // 0 for roulette wheels, 1 for ranked selection
			long sum = 0, max = 0, min = 10000;
			for (int i = 0; i < 1; i++) {
				EventRepository tempEr = new EventRepository();
				tempEr.setEds(repo.getEds());
				tempEr.setEps(repo.getEps());
				tempEr.buildHierarchy();

				EventPattern newQuery = this.query.clone();
				tempEr.getEps().put(newQuery.getID(), newQuery);
				logger.info("inserting query: " + newQuery.toString());
				tempEr.getReusabilityHierarchy().insertIntoHierarchy(newQuery);
				CompositionPlanEnumerator newCpe = new CompositionPlanEnumerator(tempEr, newQuery);
				long t1 = System.currentTimeMillis();
				GeneticAlgorithm ga = new GeneticAlgorithm(newCpe, selectionMode, populationSize, iterations, 1.0,
						mutationRate, factor, false, weight, constraint);
				List<EventPattern> results = ga.evolution();
				long t2 = System.currentTimeMillis();
				result = results.get(results.size() - 1);
				time = t2 - t1;
				sum += time;
				if (min >= time)
					min = time;
				if (max <= time)
					max = time;
				this.repo.getEps().clear();
				System.gc();
			}
			logger.info("min: " + min + ", avg: " + sum / 10 + ", max: " + max);
		} else if (mode == 2) {
			// System.out.println("========Brute-Force Algorithm Begin (run-time calculation option #1)========");
			// repo.getEps().clear();
			repo.clearHierarchy();
			repo.getEps().put(query.getID(), query);
			logger.info("inserting query: " + query.toString());
			repo.getReusabilityHierarchy().insertIntoHierarchy(query);
			result = cpe.calculateBestFitness(weight, constraint);
			// int populationSize = 300;
			// int iterations = 100;
			// Double mutationRate = 0.00;
			// Double factor = 0.0;
			// int selectionMode = 0; // 0 for roulette wheels, 1 for ranked selection
			GeneticAlgorithm ga = new GeneticAlgorithm(cpe, selectionMode, populationSize, iterations, crossoverRate,
					mutationRate, factor, fixedPopulationSize, weight, constraint);
			// System.out.println("========Genetic Algorithm Begin (run-time calculation option #2)========");
			List<EventPattern> results = ga.evolution();
			result = ga.getBestEP(results);
			// RDFFileManager.writeEPToFile("CompositionPlan-Latency.n3", result);
		} else if (mode == 3) {
			List<String> ids = new ArrayList<String>();
			ids.add("http://www.insight-centre.org/dataset/SampleEventService#230");
			ids.add("http://www.insight-centre.org/dataset/SampleEventService#228");
			ids.add("http://www.insight-centre.org/dataset/SampleEventService#226");
			ids.add("http://www.insight-centre.org/dataset/SampleEventService#220");
			ids.add("http://www.insight-centre.org/dataset/SampleEventService#218");
			ids.add("http://www.insight-centre.org/dataset/SampleEventService#216");
			ids.add("http://www.insight-centre.org/dataset/SampleEventService#540");
			ids.add("http://www.insight-centre.org/dataset/SampleEventService#537");
			ids.add("http://www.insight-centre.org/dataset/SampleEventService#322");
			logger.info("=====" + param + "=====");
			// int populationSize = 200;
			// int iterations = 10000;
			// int testIt = 30;
			// Double mutationRate = 0.0;
			// Double crossoverRate = 1.0;
			// Double factor = 0.0;
			// boolean fixedPopulationSize = false;
			// int selectionMode = 0; // 0 for roulette wheels, 1 for ranked selection
			String paramName = param.split("=")[0];
			String paramValue = param.split("=")[1];
			if (paramName.equals("m"))
				mutationRate = Double.parseDouble(paramValue);
			else if (paramName.equals("c"))
				crossoverRate = Double.parseDouble(paramValue);
			else if (paramName.equals("s"))
				selectionMode = Integer.parseInt(paramValue);
			else if (paramName.equals("p"))
				populationSize = Integer.parseInt(paramValue);
			else if (paramName.equals("pf")) {
				populationSize = Integer.parseInt(paramValue);
				fixedPopulationSize = true;
			}
			long sum = 0, max = 0, min = 10000;
			CEScores.add(new ArrayList<CEScore>());
			for (int i = 0; i < testIt + 1; i++) {
				// if (CEScores.size() > 0) {
				// double sum=0.0;
				// for(CEScore:CEScores.get(index))
				// }
				logger.info("Test Iteration: " + i);
				// if (i == 1)

				int erhSize = 50;
				EventRepository tempEr = new EventRepository();
				tempEr.setEds(repo.getEds());
				tempEr.setEps(repo.getEps());
				tempEr.getEps()
						.remove("EP-http://www.insight-centre.org/dataset/SampleEventService#SampleRequestclone");
				// logger.info("EDs: " + tempEr.getEds().size() + ", EPs: " + tempEr.getEps().size());
				/*
				 * use random CESs
				 */
				// List<EventDeclaration> simEds = ServiceGenerator.createAndEvents(ids, erhSize);
				// tempEr.getEps().clear();
				// // tempEr.getReusabilityHierarchy().getPrimitiveHierarchy().clear();
				// List<Entry<String, EventDeclaration>> edsToRemove = new ArrayList<Entry<String, EventDeclaration>>();
				// for (Entry<String, EventDeclaration> en : tempEr.getEds().entrySet()) {
				// if (en.getValue().getEp() != null) {
				// // System.out.println("removing ed: " + ed.getServiceId());
				// edsToRemove.add(en);
				// }
				// }
				// System.out.println("total eds to remove: " + edsToRemove.size());
				// System.out.println("total eds b4: " + tempEr.getEds().size());
				// for (Entry s : edsToRemove) {
				// tempEr.getEds().entrySet().remove(s);
				// // ACEISEngine.getRepo().getEps().entrySet().remove(((EventDeclaration)
				// // s.getValue()).getEp().getID());
				// }
				// System.out.println("total eds after1: " + tempEr.getEds().size());
				// for (EventDeclaration ed : simEds) {
				// tempEr.getEds().put(ed.getnodeId(), ed);
				// tempEr.getEps().put(ed.getEp().getID(), ed.getEp());
				// }
				// System.out.println("total eds after2: " + tempEr.getEds().size());
				tempEr.clearHierarchy();
				tempEr.buildHierarchy();
				EventPattern newQuery = this.query.clone();
				tempEr.getEps().put(newQuery.getID(), newQuery);
				logger.debug("inserting query: " + newQuery.toSimpleString());
				tempEr.getReusabilityHierarchy().insertIntoHierarchy(newQuery);
				CompositionPlanEnumerator newCpe = new CompositionPlanEnumerator(tempEr, newQuery);
				long t1 = System.currentTimeMillis();
				GeneticAlgorithm ga = new GeneticAlgorithm(newCpe, selectionMode, populationSize, iterations,
						crossoverRate, mutationRate, factor, fixedPopulationSize, weight, constraint);
				List<EventPattern> results = ga.evolution();
				long t2 = System.currentTimeMillis();
				result = results.get(results.size() - 1);
				time = t2 - t1;
				sum += time;
				if (min >= time)
					min = time;
				if (max <= time)
					max = time;
				// this.repo.getEps().clear();
				System.gc();
			}
			logger.info("min: " + min + ", avg: " + sum / testIt + ", max: " + max);
			time = sum / testIt;
		} else if (mode == 5) {
			// result = RDFFileManager.extractCompositionPlanFromDataset("toitRepo/CompositionPlan-Traffic.n3");
			result = RDFFileManager.extractCompositionPlanFromDataset("toitRepo/CompositionPlan-Latency.n3");
			double u = Comparator.calculateUtility(constraint, weight, result.aggregateQos());
			// WeightVector weight = new WeightVector(1.0, 0.0, 0.0, 0.0, 0.0, 0.0); // optimize latency
			// WeightVector weight = new WeightVector(0.01, 0.01, 0.01, 1.0, 0.01, 0.01); // optimize accuracy
			// WeightVector weight = new WeightVector(0.01, 0.01, 0.01, 0.01, 1.0, 0.01); // optimize completeness
			// WeightVector weight = new WeightVector(0.01, 0.01, 0.01, 0.01, 0.01, 1.0); // optimize traffic
			// WeightVector weight = new WeightVector(0.01, 1.0, 0.01, 0.01, 0.01, 0.01); // optimize latency
			// logger.info("Weight Vector: " + weight.toString());
			// QosVector constraint = new QosVector(600, 2700, 1, 0.0, 0.0, 6.0);
			// logger.info("Constraint Vector: " + constraint.toString());

			logger.info("Result loaded:  " + result.toSimpleString() + "\n u: " + u);
		}
		return result;
	}

	private void testQT(EventDeclaration compositionPlan, ACEISEngine engine) throws Exception {
		// SubscriptionManagerFactory.setContext(context);
		SubscriptionManager sub = SubscriptionManagerFactory.getSubscriptionManager();
		// WebSocketServer.runServer();
		// if (!compositions.getEds().isEmpty()) {
		// EventDeclaration newEd = compositions.getEds().values().iterator().next();
		String start = "2014-08-21";
		String end = "2014-08-25";
		Date startDate = new SimpleDateFormat("yyyy-MM-dd", Locale.ENGLISH).parse(start);
		Date endDate = new SimpleDateFormat("yyyy-MM-dd", Locale.ENGLISH).parse(end);
		sub.registerLocalEventRequest(engine, compositionPlan, startDate, endDate, "[range 1500ms step 500ms]",
				QosSimulationMode.completeness);
		QosMonitor qm = new QosMonitor(compositionPlan);
		new Thread(qm).start();
		// }
	}

	private void writeAvgUtilities(String param, long t) {
		String outputFile = "resultLog/toit/" + param + ".csv";
		List<Double> utilities = sortAvgUtilities(utilityInGenerations);
		// before we open the file check to see if it already exists
		boolean alreadyExists = new File(outputFile).exists();

		try {
			// use FileWriter constructor that specifies open for appending
			CsvWriter csvOutput = new CsvWriter(new FileWriter(outputFile, true), ',');

			// if the file didn't already exist then we need to write out the header line
			if (!alreadyExists) {
				csvOutput.write(" ");
				csvOutput.write(param);
				csvOutput.endRecord();
			}
			for (int i = 0; i < utilities.size(); i++) {
				csvOutput.write(i + "");
				csvOutput.write(utilities.get(i) + "");
				csvOutput.endRecord();
			}
			csvOutput.write("");
			csvOutput.write(t + "");
			csvOutput.endRecord();
			csvOutput.close();
		} catch (IOException e) {
			e.printStackTrace();
		}
	}

	private void writeCEScores(String header, List<String> params) {
		String outputFile = "resultLog/toit/" + header + "-cescore.csv";
		// List<Double> utilities = sortAvgUtilities(utilityInGenerations);
		// before we open the file check to see if it already exists
		boolean alreadyExists = new File(outputFile).exists();

		try {
			// use FileWriter constructor that specifies open for appending
			CsvWriter csvOutput = new CsvWriter(new FileWriter(outputFile, true), ',');

			// if the file didn't already exist then we need to write out the header line
			if (!alreadyExists) {
				csvOutput.write(" ");
				csvOutput.write("avg");
				csvOutput.write("min");
				csvOutput.write("max");
				csvOutput.write("bestUtility");
				csvOutput.write("t");
				csvOutput.write("gain");
				csvOutput.endRecord();
			}
			for (int i = 0; i < params.size(); i++) {
				csvOutput.write(params.get(i).split("=")[1] + "");
				double scoreSum = 0.0;
				double gainSum = 0.0;
				double min = 1000.0;
				double max = -1.0;
				int cnt = 0;
				double tSum = 0.0;
				double bestU = -1.0;
				for (int j = 1; j < CEScores.get(i).size(); j++) {
					CEScore ces = CEScores.get(i).get(j);
					double score = ces.getCEScoreUsingMaxUtility();
					double gain = ces.getCEScoreGainUsingMaxUtility();
					double convergedU = ces.getMaxUtility();
					long t = ces.getTime();
					tSum += t;
					scoreSum += score;
					gainSum += gain;
					if (min >= score)
						min = score;
					if (max <= score)
						max = score;
					if (bestU <= convergedU)
						bestU = convergedU;
					cnt += 1;
				}

				csvOutput.write(scoreSum / cnt + "");
				csvOutput.write(min + "");
				csvOutput.write(max + "");
				csvOutput.write(bestU + "");
				csvOutput.write(tSum / cnt + "");
				csvOutput.write(gainSum / cnt + "");
				csvOutput.endRecord();
			}
			csvOutput.close();
		} catch (IOException e) {
			e.printStackTrace();
		}
	}

	private void writeCompositionPlan(EventDeclaration ed, String fileName) throws Exception {
		Model serviceModel = ModelFactory.createDefaultModel();
		serviceModel.setNsPrefixes(RDFFileManager.prefixMap);
		// for (EventDeclaration ed : repo.getEds().values())
		serviceModel.add(RDFFileManager.createEDModel(serviceModel, ed));
		RDFWriter writer = serviceModel.getWriter("N3");
		OutputStream out = new FileOutputStream(RDFFileManager.datasetDirectory + fileName);
		writer.write(serviceModel, out, null);
		out.close();
	}
}
