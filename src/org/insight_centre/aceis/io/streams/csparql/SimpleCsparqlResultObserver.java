package org.insight_centre.aceis.io.streams.csparql;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.LinkedBlockingQueue;

import org.deri.cqels.engine.ContinuousSelect;
import org.insight_centre.aceis.engine.ACEISEngine;
import org.insight_centre.aceis.engine.ACEISScheduler;
import org.insight_centre.aceis.eventmodel.EventDeclaration;
import org.insight_centre.aceis.querytransformation.TransformationResult;
import org.insight_centre.aceis.subscriptions.SubscriptionManagerFactory;
import org.insight_centre.citypulse.main.MultipleInstanceMain;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.hp.hpl.jena.rdf.model.Model;
import com.hp.hpl.jena.rdf.model.ModelFactory;

import eu.larkc.csparql.common.RDFTable;
import eu.larkc.csparql.common.RDFTuple;
import eu.larkc.csparql.common.streams.format.GenericObservable;
import eu.larkc.csparql.engine.RDFStreamFormatter;

public class SimpleCsparqlResultObserver extends RDFStreamFormatter {
	// private Map<String, String> constraintMap;
	private ACEISEngine engine = null;
	// private JsonQuery jq;
	String[] latestResult;
	private Set<String> latestResults = new HashSet<String>();
	private static final Logger logger = LoggerFactory.getLogger(SimpleCsparqlResultObserver.class);
	private LinkedBlockingQueue<String> cache = new LinkedBlockingQueue<String>(1000);
	private LinkedBlockingQueue<String> capturedObIds = new LinkedBlockingQueue<String>(1000);
	// private Map<String, Modifier> modifierMap;
	// private Map<String, Operator> operatorMap;
	// private String qid = "";
	// private Map<String, Integer> messageCnt;
	// private File logFile;
	// private FileWriter fw;
	// private CsvWriter cw;
	// private int correct = 0, incorrect = 0;
	// private long recordTime, byteCnt, messageCnt;
	private ContinuousSelect selQuery;
	private List<String> serviceList, propertyList, sensorList, requestedProperties;
	// private Session session = null;
	private List<String> subscribers = new ArrayList<String>();
	// private QosSimulationMode qosSimulationMode = QosSimulationMode.none;
	private EventDeclaration ed;

	// private RDFTuple ;

	// private Map<String, SensorObservation> producedObservations = new HashMap<String, SensorObservation>();

	// public Map<String, SensorObservation> getProducedObservations() {
	// return producedObservations;
	// }

	public SimpleCsparqlResultObserver(ACEISEngine engine, String iri, TransformationResult tr) {
		super(iri);
		// this.session = session;
		this.engine = engine;
		this.sensorList = tr.getSensorList();
		// recordTime = System.currentTimeMillis();
		// byteCnt = 0;
		// messageCnt = 0;
		// recordTime = System.currentTimeMillis();
		this.serviceList = tr.getServiceList();
		this.propertyList = tr.getPropertyList();
		// this.jq = jq;
		// if (this.jq != null) {
		// this.constraintMap = jq.getConstraintMap();
		// this.modifierMap = jq.getModifierMap();
		// this.operatorMap = jq.getOperatorMap();
		// this.requestedProperties = jq.getProperties();
		// }
		this.ed = tr.getTransformedFrom();
		// if (this.ed.getInternalQos() == null)
		// logger.warn("NULL internal qos: " + ed.getServiceId());
		// else
		// logger.warn(ed.getServiceId().split("#")[1] + " internal qos: " + ed.getInternalQos());
		// this.qid = tr.getQid();
		// this.subscribers.add(qid);
		// tr.
		// this
	}

	public List<String> getSubscribers() {
		return subscribers;
	}

	private void streamResultAsTriples(String[] resultArray) throws IOException, ExecutionException,
			InterruptedException {

		int arraySize = resultArray.length;
		if (arraySize % 2 != 0)
			logger.error(this.getIRI() + ": Result size incorrect. " + Arrays.asList(resultArray));
		else {
			long receivedTime = System.currentTimeMillis();
			Model m = ModelFactory.createDefaultModel();
			Map<String, Long> latencies = new HashMap<String, Long>();
			for (int i = 0; i < resultArray.length; i = i + 2) {
				String obIdStr = resultArray[i];
				if (!capturedObIds.contains(obIdStr)) {
					if (capturedObIds.remainingCapacity() <= 1) {
						capturedObIds.poll();
					}
					capturedObIds.put(obIdStr);
					// logger.info("looking for: " + obIdStr);
					try {
						// long initTime = ACEISScheduler.getObMap().get(obIdStr.split("#")[1]);
						long t1 = System.currentTimeMillis();
						long initTime = ACEISScheduler.getObMap().get(obIdStr.split("#")[1]);
						long t2 = System.currentTimeMillis();
						if (t2 - t1 >= 100) {
							logger.warn("Cache lookup time: " + (t2 - t1));
						}
						latencies.put(obIdStr, (receivedTime - initTime));
					} catch (Exception e) {
						logger.warn("Missing obId in cache.");
					}
				}
				if (latencies.size() > 0)
					MultipleInstanceMain.getMonitor().addResults(getIRI(), latencies, 1);

				// String sensorIdStr = sensorList.get(i / 2).replaceAll("<", "").replaceAll(">", "");
				// String propertyIdStr = propertyList.get(i / 2).replaceAll("<", "").replaceAll(">", "");
				// Resource propertyId = m.createResource(propertyIdStr);
				// String valueStr = resultArray[i + 1];
				// Resource observation = m.createResource(obIdStr);
				// Property hasValue = m.createProperty(VirtuosoDataManager.saoPrefix + "hasValue");
				// observation.addProperty(RDF.type, m.createResource(VirtuosoDataManager.ssnPrefix + "Observation"));
				// Resource sensorId = m.createResource(sensorIdStr);
				// observation.addProperty(m.createProperty(VirtuosoDataManager.ssnPrefix + "observedBy"), sensorId);
				// observation.addProperty(m.createProperty(VirtuosoDataManager.ssnPrefix + "observedProperty"),
				// propertyId);
				// if (valueStr.contains("^^")) {
				// if (valueStr.contains("double"))
				// observation.addLiteral(hasValue, m.createTypedLiteral(Double.parseDouble(valueStr.substring(1,
				// valueStr.indexOf("^^") - 1))));
				// else if (valueStr.contains("string"))
				// observation.addLiteral(hasValue,
				// m.createTypedLiteral(valueStr.substring(1, valueStr.indexOf("^^") - 1)));
				// } else
				// observation.addProperty(hasValue, m.createResource(valueStr));
			}
			if (!MultipleInstanceMain.getMonitor().isStarted()) {
				new Thread(MultipleInstanceMain.getMonitor()).start();
				logger.info("Monitor thread started.");
			}

			// if (isNew) {
			// byteCnt += resultArray.toString().getBytes().length;
			// messageCnt += 1;
			// latestResult = resultArray;

			// long messageByte = 0;
			// logger.debug("Result array: " + Arrays.asList(resultArray));

			// byteCnt += messageByte;
			// latestResult = resultArray;

			// List<Statement> stmts = m.listStatements().toList();
			// for (Statement st : stmts) {
			// final RdfQuadruple q = new RdfQuadruple(st.getSubject().toString(), st.getPredicate().toString(), st
			// .getObject().toString(), System.currentTimeMillis());
			// this.put(q);
			// // logger.info(this.getIRI() + " Streaming Result: " + st.toString());
			// }

		}
	}

	public void setSubscribers(List<String> subscribers) {
		this.subscribers = subscribers;
	}

	public synchronized void addSubscriber(String subscriber) {
		this.subscribers.add(subscriber);
	}

	@Override
	public void update(final GenericObservable<RDFTable> observed, final RDFTable q) {
		// logger.info(this.getIRI().split("#")[1] + ": " + q.size() + " updates received.");
		for (final RDFTuple t : q) {
			try {
				String result = t.toString().replaceAll("\t", " ").trim();
				if (!cache.contains(result)) {
					// logger.info("Csparql result: " + result);
					if (cache.remainingCapacity() <= 1)
						cache.poll();
					cache.put(result);
					// this.latestResults.add(result);
					SubscriptionManagerFactory
							.getSubscriptionManager()
							.getMessageCnt()
							.put(this.getIRI(),
									SubscriptionManagerFactory.getSubscriptionManager().getMessageCnt()
											.get(this.getIRI()) + 1);
					String[] results = result.split(" ");

					streamResultAsTriples(results);
				} else {
					// logger.warn("Duplicated.");
				}
			} catch (IOException | InterruptedException | ExecutionException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
			// logger.debug(this.getIRI() + " Streaming: " + result);
		}
	}
}
