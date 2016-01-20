package org.insight_centre.aceis.engine;

import java.net.URI;
import java.util.UUID;

import org.deri.cqels.engine.ExecContext;
import org.insight_centre.aceis.io.EventRepository;
import org.insight_centre.aceis.io.rdf.VirtuosoDataManager;
import org.insight_centre.aceis.subscriptions.SubscriptionManager;
import org.insight_centre.aceis.subscriptions.SubscriptionManagerFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.hp.hpl.jena.reasoner.ReasonerRegistry;

import eu.larkc.csparql.engine.CsparqlEngine;
import eu.larkc.csparql.engine.CsparqlEngineImpl;

public class ACEISEngine {
	private ExecContext cqelsContext = null;
	private EventRepository er;
	private SubscriptionManager subscriptionMgr;
	private CsparqlEngine csparqlEngine = null;
	// public static CompositionPlanEnumerator cpe;
	private RspEngine engineType;
	private final String id;
	private static final Logger logger = LoggerFactory.getLogger(ACEISEngine.class);

	public RspEngine getEngineType() {
		return engineType;
	}

	public void setEngineType(RspEngine engineType) {
		this.engineType = engineType;
	}

	private boolean initialized = false;

	public enum RspEngine {
		CQELS, CSPARQL;
	}

	public ExecContext getContext() {
		return cqelsContext;
	}

	public CsparqlEngine getCsparqlEngine() {
		return csparqlEngine;
	}

	public EventRepository getRepo() {
		return er;
	}

	// public SubscriptionManager getSubscriptionManager() {
	// return subscriptionMgr;
	// }

	public boolean initialized() {
		return initialized;
	}

	public ACEISEngine(RspEngine engine) {
		this.id = "ACEIS-" + UUID.randomUUID();
		this.engineType = engine;
		this.setSubscriptionMgr(SubscriptionManagerFactory.getSubscriptionManager());
	}

	// public void initialize() throws Exception {
	// // engine = m;
	// // cqelsContext = RDFFileManager.initializeCQELSContext("Scenario1Sensors.n3",
	// // ReasonerRegistry.getRDFSReasoner());
	// // RDFFileManager.initializeDataset("adptRepo/simrepo-100.n3");
	// // cqelsContext = RDFFileManager.initializeContext("jws/Scenario1Sensors.n3",
	// // ReasonerRegistry.getRDFSReasoner());
	// // cqelsContext = RDFFileManager.initializeContext("simrepo-100.n3", ReasonerRegistry.getRDFSReasoner());
	// if (engineType.equals(RspEngine.CSPARQL)) {
	// csparqlEngine = new CsparqlEngineImpl();
	// csparqlEngine.initialize(true);
	// // subscriptionMgr = SubscriptionManagerFactory.getSubscriptionManager();
	// }
	// // else
	// // cqelsContext = VirtuosoDataManager.initializeCQELSContext(dataset, null);
	// // subscriptionMgr = new SubscriptionManager(cqelsContext, null);
	// // subscriptionMgr.setAdptMode(AdaptationMode.incremental);
	// int simSize = 0;
	// er = RDFFileManager.buildRepoFromFile(simSize);
	// for (EventDeclaration ed : er.getEds().values()) {
	// ed.setInternalQos(QosVector.getRandomQos());
	// ed.setFrequency(5.0);
	// }
	// // RDFFileManager.writeRepoToFile("jws/SimRepo-" + simSize + ".n3", er);
	// // System.exit(0);
	// er.buildHierarchy();
	// // System.out.println(er.getReusabilityHierarchy().getIdentical());
	// initialized = true;
	// // cpe = new CompositionPlanEnumerator(er, null);
	// }

	public void initialize(String dataset) throws Exception {
		if (engineType.equals(RspEngine.CSPARQL)) {
			csparqlEngine = new CsparqlEngineImpl();
			csparqlEngine.initialize(true);
			// subscriptionMgr = new SubscriptionManager(null, csparqlEngine);
		} else
			cqelsContext = VirtuosoDataManager.initializeCQELSContext(dataset, null);
		int simSize = 5;
		er = VirtuosoDataManager.buildRepoFromLocalFile(dataset, simSize);
		er.buildHierarchy();
		initialized = true;
		// cpe = new CompositionPlanEnumerator(er, null);
	}

	public void initialize(String dataset, int simSize) throws Exception {
		if (engineType.equals(RspEngine.CSPARQL)) {
			csparqlEngine = new CsparqlEngineImpl();
			csparqlEngine.initialize(true);
			// subscriptionMgr = new SubscriptionManager(null, csparqlEngine);
		} else {
			cqelsContext = VirtuosoDataManager.initializeCQELSContext(dataset, ReasonerRegistry.getRDFSReasoner());
			// subscriptionMgr = new SubscriptionManager(cqelsContext, null);
		}
		if (ACEISFactory.getBaseER() == null) {
			er = VirtuosoDataManager.buildRepoFromLocalFile(dataset, simSize);
			er.buildHierarchy();
			ACEISFactory.setBaseER(er.clone());
		} else
			er = ACEISFactory.getBaseER().clone();
		initialized = true;
	}

	public void initialize(URI uri) throws Exception {
		if (engineType.equals(RspEngine.CSPARQL)) {
			csparqlEngine = new CsparqlEngineImpl();
			csparqlEngine.initialize(true);
			// subscriptionMgr = new SubscriptionManager(null, csparqlEngine);
		} else {
			// cqelsContext = VirtuosoDataManager.initializeCQELSContext("TrafficStaticData.n3",
			// ReasonerRegistry.getRDFSReasoner());
			cqelsContext = new ExecContext(VirtuosoDataManager.cqelsHome, true);
		}
		int simSize = 5;
		if (ACEISFactory.getBaseER() == null) {
			er = VirtuosoDataManager.buildRepoFromSparqlEndpoint(uri, simSize);
			er.buildHierarchy();
			ACEISFactory.setBaseER(er.clone());
		} else
			er = ACEISFactory.getBaseER().clone();
		initialized = true;
	}

	public String getId() {
		return id;
	}

	public SubscriptionManager getSubscriptionMgr() {
		return subscriptionMgr;
	}

	public void setSubscriptionMgr(SubscriptionManager subscriptionMgr) {
		this.subscriptionMgr = subscriptionMgr;
	}
}
