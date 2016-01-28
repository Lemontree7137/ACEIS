package org.insight_centre.aceis.utils.test;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.insight_centre.aceis.engine.Comparator;
import org.insight_centre.aceis.engine.CompositionPlanEnumerator;
import org.insight_centre.aceis.engine.GeneticAlgorithm;
import org.insight_centre.aceis.engine.UtilityRanker;
import org.insight_centre.aceis.eventmodel.EventDeclaration;
import org.insight_centre.aceis.eventmodel.EventOperator;
import org.insight_centre.aceis.eventmodel.EventPattern;
import org.insight_centre.aceis.eventmodel.OperatorType;
import org.insight_centre.aceis.eventmodel.QosVector;
import org.insight_centre.aceis.eventmodel.WeightVector;
import org.insight_centre.aceis.io.EventRepository;
import org.insight_centre.aceis.io.rdf.RDFFileManager;

import com.csvreader.CsvReader;

public class Test {
	private String repoPath, queryPath;

	public Test(String repoPath, String queryPath) {
		super();
		this.repoPath = repoPath;
		this.queryPath = queryPath;
	}

	public String getRepoPath() {
		return repoPath;
	}

	public void setRepoPath(String repoPath) {
		this.repoPath = repoPath;
	}

	public String getQueryPath() {
		return queryPath;
	}

	public void setQueryPath(String queryPath) {
		this.queryPath = queryPath;
	}

	private static void createStreams() throws IOException {
		File dir = new File("../CityPulseDataset/Aarhus Traffic Dataset/DataJuneSeptember2014");
		Map<String, String> idMap = new HashMap<String, String>();
		CsvReader metaData = new CsvReader("dataset/MetaData/trafficMetaData.csv");
		metaData.readHeaders();
		while (metaData.readRecord()) {
			String id = metaData.get("extID");
			String rid = metaData.get("REPORT_ID");
			idMap.put(rid, id);
			// stream(n(RDFFileManager.defaultPrefix + streamData.get("REPORT_ID")),
			// n(RDFFileManager.ctPrefix + "hasETA"), n(data.getEstimatedTime() + ""));
			// System.out.println("metadata: " + metaData.toString());

		}
		metaData.close();
		File[] directoryListing = dir.listFiles();
		if (directoryListing != null) {
			for (File child : directoryListing) {
				// if (child.getName().length() <= 11)
				// continue;
				System.out.println(child.getName());
				child.renameTo(new File(idMap.get(child.getName().split("\\.")[0].substring(11)) + ".stream"));
			}
		} else {
			// Handle the case where dir is not really a directory.
			// Checking dir.isDirectory() above would not be sufficient
			// to avoid race conditions with another process that deletes
			// directories.
		}
	}

	private static void createWeatherStream() throws IOException {
		File weatherStream = new File("../CityPulseDataset/Aarhus Weather Dataset");
	}

	private static void createPollutionStreams() throws IOException {
		File dir = new File(
				"../CityPulseDataset/Aarhus Weather Dataset/August-September/csv format/weatherDataOctoberSeptember.csv");
		// File[] directoryListing = dir.listFiles();
		// if (directoryListing != null) {
		// for (File child : directoryListing) {
		String fn = dir.getName();
		System.out.println(fn);
		// dir.renameTo(new File("streams/" + fn.split("\\.")[0] + ".stream"));
		File newStream = new File("streams/" + fn.split("\\.")[0] + ".stream");
		Files.copy(dir.toPath(), newStream.toPath());
		// }
		// } else {
		// Handle the case where dir is not really a directory.
		// Checking dir.isDirectory() above would not be sufficient
		// to avoid race conditions with another process that deletes
		// directories.
		// }
		// System.out.println(first.getName());
		// for (String s : rids) {
		//
		// File newStream = new File("streams/pollutionData" + s + ".stream");
		// Files.copy(first.toPath(), newStream.toPath());
		// }
	}

	public static void main(String[] args) throws Exception {

		for (int n = 1; n < 10000; n++) {
			Double n_1 = 1.0;
			for (int i = 1; i < n + 1; i++)
				n_1 = n_1 * i;
			Double d1 = Math.pow(n, n);
			Double d2 = Math.pow(Math.E, n);
			Double d = d1 / d2;
			System.out.println("n=" + n + ", " + n_1 + ", " + d + " " + (n_1 > d));
		}
		// createStreams();
		// createPollutionStreams();
		// System.out.println(Double.parseDouble("12"));
		// ARQ.set(ARQ.outputGraphBNodeLabels, true);
		// // RDFFileManager.initializeDataset("TrafficSensors.n3");
		// ExecContext context = RDFFileManager.initializeContext("TrafficSensors.n3",
		// ReasonerRegistry.getRDFSReasoner());
		// EventRepository er = RDFFileManager.buildRepoFromFile();
		// // System.out.println(er.getEds().size());
		// EventPattern query = RDFFileManager.extractQueryFromDataset("I2-event_request.n3");
		// // for (EventDeclaration qed : query.getEds())
		// // System.out.println("QUERY: " + query);
		// er.getEps().put(query.getID(), query);
		// er.buildHierarchy();
		// // System.out.println("ER: eds:" + er.getEds().size() + " eps: " + er.getEps().size());
		// // System.out.println("Reusability Hierarchy:" + er.getHierarchy());
		//
		// CompositionPlanEnumerator cpe = new CompositionPlanEnumerator(er, query);
		// List<EventPattern> results = cpe.getCCPsForQuery(query);
		// System.out.println("CCP result size: " + results.size());
		// String edId = query.getID().replaceAll("EP-", "");
		//
		// EventRepository compositions = new EventRepository();
		// for (EventPattern ep : results) {
		// EventDeclaration compositionPlan = new EventDeclaration(edId + "-" + ep.getID(), "", "complex", ep, null,
		// null);
		// compositionPlan.setComposedFor(query);
		// // System.out.println("ED created: " + compositionPlan);
		// compositions.getEds().put(compositionPlan.getnodeId(), compositionPlan);
		// }
		// RDFFileManager.writeRepoToFile("CompositionPlans", compositions);
		// RDFFileManager.closeDataset();
		//
		// SubscriptionManager sub = new SubscriptionManager(context);
		// if (!compositions.getEds().isEmpty()) {
		// EventDeclaration chosenPlan = compositions.getEds().values().iterator().next();
		// sub.registerEventRequest(chosenPlan, 0, null, null);
		// }
		// Model m = FileManager.get().loadModel("dataset/serviceCategories.n3");
		// System.out.print(m);
		// System.out.println.("roots: "+er.getHierarchy());
		// RDFFileManager.writeRepoToFile("servicerepository.n3", er);
		// EventPattern query = RDFFileManager.extractQueryFromDataset("SampleEventRequest.n3");
		// QosVector constraint = new QosVector();
		// WeightVector weight = new WeightVector();
		// RDFFileManager.extractConstraintAndPreferenceById(query.getID(), constraint, weight);
		// RDFFileManager.closeDataset();

		// System.out.println(results.size());
		// System.out.println(constraint + "\n" + weight);
	}

	public List<EventPattern> compose(boolean useBruteForce) throws Exception {
		RDFFileManager.initializeDataset();
		EventRepository er = RDFFileManager.buildRepoFromFile(0);
		er.buildHierarchy();

		EventPattern query = RDFFileManager.extractQueryFromDataset(queryPath);
		QosVector constraint = new QosVector();
		WeightVector weight = new WeightVector();
		RDFFileManager.extractConstraintAndPreferenceById(query.getID(), constraint, weight);
		RDFFileManager.closeDataset();

		List<EventPattern> results = new ArrayList<EventPattern>();
		CompositionPlanEnumerator cpe = new CompositionPlanEnumerator(er, query);
		if (useBruteForce) {
			results = cpe.getCCPsForQuery(query);
			List<EventPattern> filteredResults = Comparator.constraintEvaluation(results, constraint);
			results = UtilityRanker.rank(filteredResults, constraint, weight);
		} else {
			int populationSize = 100;
			int iterations = 100;
			Double mutationRate = 0.05;
			Double factor = 0.3;
			int selectionMode = 0; // 0 for roulette wheels, 1 for ranked selection
			GeneticAlgorithm ga = new GeneticAlgorithm(cpe, selectionMode, populationSize, iterations, 1.0,
					mutationRate, factor, false, weight, constraint);
			results = ga.evolution();
		}
		return results;

	}

	private static EventDeclaration createComplexEDwithAndOperator(String id, List<String> subevents, EventRepository er) {
		List<EventDeclaration> subEvents = new ArrayList<EventDeclaration>();
		for (String s : subevents)
			subEvents.add(er.getEds().get(s));
		// System.out.println("subevents: " + subEvents.size());
		EventDeclaration result = new EventDeclaration(id, "", "complex", null, null, null, null);
		EventPattern ep = new EventPattern();
		ep.setID("EP-" + result.getnodeId());
		ep.setEds(subEvents);
		ep.getEos().add(new EventOperator(OperatorType.and, 1, "0"));
		ep.getProvenanceMap().put("0", subevents);
		result.setEp(ep);
		er.getEds().put(result.getnodeId(), result);
		er.getEps().put(ep.getID(), ep);
		return result;

	}

	private static EventDeclaration createComplexEDwithOrOperator(String id, List<String> subevents, EventRepository er) {
		List<EventDeclaration> subEvents = new ArrayList<EventDeclaration>();
		for (String s : subevents)
			subEvents.add(er.getEds().get(s));
		// System.out.println("subevents: " + subEvents.size());
		EventDeclaration result = new EventDeclaration(id, "", "complex", null, null, null, null);
		EventPattern ep = new EventPattern();
		ep.setID("EP-" + result.getnodeId());
		ep.setEds(subEvents);
		ep.getEos().add(new EventOperator(OperatorType.and, 1, "0"));
		ep.getProvenanceMap().put("0", subevents);
		result.setEp(ep);
		er.getEds().put(result.getnodeId(), result);
		er.getEps().put(ep.getID(), ep);
		return result;

	}
}
