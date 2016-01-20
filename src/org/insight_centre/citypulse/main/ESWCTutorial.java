package org.insight_centre.citypulse.main;

import java.io.BufferedReader;
import java.io.File;
import java.io.InputStreamReader;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.UUID;

import org.insight_centre.aceis.engine.ACEISEngine;
import org.insight_centre.aceis.engine.ACEISEngine.RspEngine;
import org.insight_centre.aceis.engine.ACEISFactory;
import org.insight_centre.aceis.eventmodel.EventDeclaration;
import org.insight_centre.aceis.eventmodel.EventRequest;
import org.insight_centre.aceis.io.rdf.VirtuosoDataManager;
import org.insight_centre.aceis.subscriptions.SubscriptionManagerFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ESWCTutorial {
	private ACEISEngine engine;
	private RspEngine engineMode;
	private int candidates;
	private File eventRequestFile;
	private String queryPath;
	private boolean useBruteForce;
	private int populationSize = 100;
	private double crossover = 1.0, mutation = 0.03;
	private static final Logger logger = LoggerFactory.getLogger(ESWCTutorial.class);

	public ESWCTutorial(HashMap<String, String> parameters) throws Exception {
		if (parameters.get("engine") == null) {
			throw new Exception("Engine not specified.");
		} else if (parameters.get("engine").equals(RspEngine.CSPARQL.toString()))
			this.engineMode = RspEngine.CSPARQL;
		else if (parameters.get("engine").equals(RspEngine.CQELS.toString()))
			this.engineMode = RspEngine.CQELS;
		else
			throw new Exception("Specified engine not supported.");

		if (parameters.get("query") == null)
			throw new Exception("Event request not specified.");
		else {
			eventRequestFile = new File(parameters.get("query"));
			if (!eventRequestFile.exists())
				throw new Exception("Event request not found.");
			queryPath = parameters.get("query");
		}
		if (parameters.get("candidates") == null) {
			this.candidates = 0;
		} else {
			this.candidates = Integer.parseInt(parameters.get("candidates"));
		}

		if (parameters.get("alg") == null)
			throw new Exception("Test algorithm not specified.");
		else if (parameters.get("alg").equals("bf"))
			this.useBruteForce = true;
		else if (parameters.get("alg").equals("ga"))
			this.useBruteForce = false;
		else
			throw new Exception("Test algorithm not supported.");
		if (parameters.get("population") != null) {
			this.populationSize = Integer.parseInt(parameters.get("population"));
			if (this.populationSize < 50)
				throw new Exception("Initial population size too small (< 50).");
		}
		if (parameters.get("crossover") != null) {
			this.crossover = Double.parseDouble(parameters.get("crossover"));
			if (crossover <= 0 || crossover > 1)
				throw new Exception("Cross over rate out of range (0,1]");
		}
		if (parameters.get("mutation") != null) {
			this.mutation = Double.parseDouble(parameters.get("mutation"));
			if (mutation <= 0 || mutation >= 1)
				throw new Exception("Mutation rate out of range (0,1)");
		}
		ACEISEngine engine = ACEISFactory.createACEISInstance(RspEngine.CSPARQL, null);
		engine.initialize("TrafficStaticData.n3", candidates);
		this.engine = engine;

	}

	private void startTest() throws Exception {
		EventRequest evtRequest = VirtuosoDataManager.extractEventRequestFromFile(queryPath);
		EventDeclaration compPlan = SubscriptionManagerFactory.getSubscriptionManager().createCompositionPlan(engine,
				evtRequest.getEp(), evtRequest.getConstraint(), evtRequest.getWeight(), false, this.useBruteForce,
				this.populationSize, this.crossover, this.mutation);
		List<EventDeclaration> plans = new ArrayList<EventDeclaration>();
		plans.add(compPlan);
		VirtuosoDataManager.writeEDsToFile("results/CompositionPlan-" + UUID.randomUUID(), plans);
		SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd");
		Date start = sdf.parse("2014-08-01");
		Date end = sdf.parse("2014-08-31");
		SubscriptionManagerFactory.getSubscriptionManager().registerLocalEventRequest(engine, compPlan, start, end,
				null, null);
	}

	public static void main(String[] args) throws Exception {
		HashMap<String, String> parameters = new HashMap<String, String>();
		for (String s : args) {
			parameters.put(s.split("=")[0], s.split("=")[1]);
		}
		ESWCTutorial test = new ESWCTutorial(parameters);
		test.startTest();
		try {
			// server2.start();
			BufferedReader reader = new BufferedReader(new InputStreamReader(System.in));
			System.out.println("Please press enter to stop the ACEIS server.");
			reader.readLine();
		} catch (Exception e) {
			throw new RuntimeException(e);
		} finally {
			System.exit(0);
			// server2.stop();
		}

	}
}
