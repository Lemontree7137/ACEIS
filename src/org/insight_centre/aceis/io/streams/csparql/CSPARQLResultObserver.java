package org.insight_centre.aceis.io.streams.csparql;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import javax.websocket.Session;

import org.deri.cqels.engine.ContinuousSelect;
import org.insight_centre.aceis.engine.ACEISEngine;
import org.insight_centre.aceis.eventmodel.EventDeclaration;
import org.insight_centre.aceis.io.rdf.RDFFileManager;
import org.insight_centre.aceis.observations.AarhusTrafficObservation;
import org.insight_centre.aceis.observations.SensorObservation;
import org.insight_centre.aceis.querytransformation.TransformationResult;
import org.insight_centre.aceis.subscriptions.SubscriptionManagerFactory;
import org.insight_centre.aceis.utils.test.Simulator2.QosSimulationMode;
import org.insight_centre.citypulse.commons.data.json.JsonQuery;
import org.insight_centre.citypulse.commons.data.json.JsonQueryResult;
import org.insight_centre.citypulse.commons.data.json.JsonQueryResults;
import org.insight_centre.citypulse.commons.data.json.Location;
import org.insight_centre.citypulse.commons.data.json.Segment;
import org.insight_centre.citypulse.server.SubscriberServerEndpoint;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.gson.Gson;
import com.hp.hpl.jena.rdf.model.Model;
import com.hp.hpl.jena.rdf.model.ModelFactory;
import com.hp.hpl.jena.rdf.model.Property;
import com.hp.hpl.jena.rdf.model.Resource;
import com.hp.hpl.jena.rdf.model.Statement;
import com.hp.hpl.jena.vocabulary.RDF;
import com.siemens.citypulse.resources.GlobalVariables.Modifier;
import com.siemens.citypulse.resources.GlobalVariables.Operator;

import eu.larkc.csparql.cep.api.RdfQuadruple;
import eu.larkc.csparql.common.RDFTable;
import eu.larkc.csparql.common.RDFTuple;
import eu.larkc.csparql.common.streams.format.GenericObservable;
import eu.larkc.csparql.engine.RDFStreamFormatter;

public class CSPARQLResultObserver extends RDFStreamFormatter {
	private Map<String, String> constraintMap;
	private ACEISEngine engine = null;
	private JsonQuery jq;
	String[] latestResult;
	private Set<String> latestResults = new HashSet<String>();
	private static final Logger logger = LoggerFactory.getLogger(CSPARQLResultObserver.class);
	private Map<String, Modifier> modifierMap;
	private Map<String, Operator> operatorMap;
	// private String qid = "";
	// private Map<String, Integer> messageCnt;
	// private File logFile;
	// private FileWriter fw;
	// private CsvWriter cw;
	private int correct = 0, incorrect = 0;
	private long recordTime, byteCnt, messageCnt;
	private ContinuousSelect selQuery;
	private List<String> serviceList, propertyList, requestedProperties;
	private Session session = null;
	private List<String> subscribers = new ArrayList<String>();
	private QosSimulationMode qosSimulationMode = QosSimulationMode.none;
	private EventDeclaration ed;
	private Map<String, SensorObservation> producedObservations = new HashMap<String, SensorObservation>();

	public Map<String, SensorObservation> getProducedObservations() {
		return producedObservations;
	}

	public CSPARQLResultObserver(ACEISEngine engine, String iri, TransformationResult tr, Session session, JsonQuery jq) {
		super(iri);
		this.session = session;
		this.engine = engine;
		// recordTime = System.currentTimeMillis();
		byteCnt = 0;
		messageCnt = 0;
		recordTime = System.currentTimeMillis();
		this.serviceList = tr.getServiceList();
		this.propertyList = tr.getPropertyList();
		this.jq = jq;
		if (this.jq != null) {
			this.constraintMap = jq.getConstraintMap();
			this.modifierMap = jq.getModifierMap();
			this.operatorMap = jq.getOperatorMap();
			this.requestedProperties = jq.getProperties();
		}
		this.ed = tr.getTransformedFrom();
		if (this.ed.getInternalQos() == null)
			logger.warn("NULL internal qos: " + ed.getServiceId());
		else
			logger.warn(ed.getServiceId().split("#")[1] + " internal qos: " + ed.getInternalQos());
		// this.qid = tr.getQid();
		// this.subscribers.add(qid);
		// tr.
		// this
	}

	private String getAggregatedValue(Modifier modifier, List<String> values) throws Exception {
		// String result = "";
		switch (modifier) {
		case MAX: {
			double max = 0.0;
			for (String s : values) {
				if (Double.parseDouble(s) >= max)
					max = Double.parseDouble(s);
			}
			return max + "";
		}
		case MIN: {
			double min = 1000.0;
			for (String s : values) {
				if (Double.parseDouble(s) <= min)
					min = Double.parseDouble(s);
			}
			return min + "";
		}
		case AVG: {
			double sum = 0.0;
			for (String s : values) {
				sum += Double.parseDouble(s);
			}
			return sum / values.size() + "";
		}
		case SUM: {
			double sum = 0.0;
			for (String s : values) {
				sum += Double.parseDouble(s);
			}
			return sum + "";
		}
		}
		throw new Exception("Unidentifiable modifier.");
	}

	private Segment getPathSegment() {
		Double startLat = this.jq.getMsg().getStartPoint().getLat();
		Double startLon = this.jq.getMsg().getStartPoint().getLng();
		Double endLat = this.jq.getMsg().getEndPoint().getLat();
		Double endLon = this.jq.getMsg().getEndPoint().getLng();
		Location start = new Location();
		start.setLat(startLat);
		start.setLon(startLon);
		Location end = new Location();
		end.setLat(endLat);
		end.setLon(endLon);
		Segment result = new Segment(start, end);
		return result;
	}

	private Segment getSegment(String foiStr) {
		Double startLat = Double.parseDouble(foiStr.split("-")[0].split(",")[0]);
		Double startLon = Double.parseDouble(foiStr.split("-")[0].split(",")[1]);
		Double endLat = Double.parseDouble(foiStr.split("-")[1].split(",")[0]);
		Double endLon = Double.parseDouble(foiStr.split("-")[1].split(",")[1]);
		Location start = new Location();
		start.setLat(startLat);
		start.setLon(startLon);
		Location end = new Location();
		end.setLat(endLat);
		end.setLon(endLon);
		Segment result = new Segment(start, end);
		return result;

	}

	public List<String> getSubscribers() {
		return subscribers;
	}

	private boolean isInThreshold(String valueStr, Operator op, String constStr) throws Exception {
		if (op == null)
			return true;
		double valueDouble = Double.parseDouble(valueStr);
		double constDouble = Double.parseDouble(constStr);
		switch (op) {
		case DIFFERENTTO: {
			if (valueDouble != constDouble)
				return true;
			else
				return false;
		}
		case EQUALTO: {
			if (valueDouble == constDouble)
				return true;
			else
				return false;
		}
		case GREATERTHAN: {
			if (valueDouble > constDouble)
				return true;
			else
				return false;

		}
		case LESSTHAN: {
			if (valueDouble < constDouble)
				return true;
			else
				return false;

		}
		case GREATERTHANOREQUALTO: {
			if (valueDouble >= constDouble)
				return true;
			else
				return false;

		}
		case LESSTHANOREQUALTO: {
			if (valueDouble <= constDouble)
				return true;
			else
				return false;

		}
		}
		throw new Exception("Unidentifiable operator.");
	}

	private void sendResultToEngine(String[] resultArray) {
		int arraySize = resultArray.length;
		if (arraySize % 2 != 0)
			logger.error(this.getIRI() + ": Result size incorrect. " + Arrays.asList(resultArray));
		else {
			Model m2 = ModelFactory.createDefaultModel();
			boolean valid = true;
			List<SensorObservation> obs = new ArrayList<SensorObservation>();
			for (int i = 0; i < resultArray.length; i = i + 2) {
				Model m = ModelFactory.createDefaultModel();
				String obIdStr = resultArray[i];

				String serviceIdStr = serviceList.get(i / 2).replaceAll("<", "").replaceAll(">", "");
				// String typeStr = resultArray[i + 1];
				String valueStr = resultArray[i + 1];
				Resource observation;
				if (!obIdStr.contains(RDFFileManager.defaultPrefix))
					observation = m.createResource(RDFFileManager.defaultPrefix + obIdStr);
				else
					observation = m.createResource(obIdStr);
				Property hasValue = m.createProperty(RDFFileManager.saoPrefix + "hasValue");
				observation.addProperty(RDF.type, m.createResource(RDFFileManager.ssnPrefix + "Observation"));
				Resource serviceID = m.createResource(serviceIdStr);
				observation.addProperty(m.createProperty(RDFFileManager.ssnPrefix + "observedBy"), serviceID);
				Resource propertyType = m.createResource(propertyList.get(i / 2));
				observation.addProperty(m.createProperty(RDFFileManager.ssnPrefix + "observedProperty"), propertyType);
				Double value = 0.0;
				// Resource value;
				if (valueStr.contains("^^")) {
					if (valueStr.contains("double")) {
						value = Double.parseDouble(valueStr.substring(1, valueStr.indexOf("^^") - 1));
						observation.addLiteral(hasValue, m.createTypedLiteral(value));
					} else if (valueStr.contains("string"))
						observation.addLiteral(hasValue,
								m.createTypedLiteral(valueStr.substring(1, valueStr.indexOf("^^") - 1)));
				} else
					observation.addProperty(hasValue, m.createResource(valueStr));
				SensorObservation so = new SensorObservation();
				so.setObId(obIdStr);
				so.setServiceId(serviceIdStr);
				so.setpType(propertyType.toString());
				so.setValue(value);
				if (!this.getProducedObservations().containsKey(so.getObId())) {
					// logger.info("adding produced: " + so.toString());
					obs.add(so);
					m2.add(m);
				} else {
					valid = false;
					break;
				}

			}
			if (valid) {
				boolean correct = true;
				for (SensorObservation so : obs) {
					this.producedObservations.put(so.getObId(), so);
					AarhusTrafficObservation raw = (AarhusTrafficObservation) SubscriptionManagerFactory
							.getSubscriptionManager().getObMap().get(so.getObId());
					if (!so.getValue().equals(raw.getVehicle_count())) {
						correct = false;
					}
				}
				if (correct)
					this.correct += 1;
				else
					this.incorrect += 1;
				List<Statement> stmts = m2.listStatements().toList();
				for (String subscriber : this.getSubscribers()) {
					SubscriptionManagerFactory.getSubscriptionManager().addConsumedMsgCnt(subscriber);
				}

				if (this.qosSimulationMode == QosSimulationMode.latency) {
					if (this.ed.getInternalQos() != null)
						try {
							Thread.sleep(this.ed.getInternalQos().getLatency());
						} catch (InterruptedException e) {
							// TODO Auto-generated catch block
							e.printStackTrace();
						}
				}
				boolean send = true;
				if (this.qosSimulationMode == QosSimulationMode.completeness) {
					if (this.ed.getInternalQos() != null)
						if (Math.random() > this.ed.getInternalQos().getReliability()) {
							send = false;
						}
				}
				if (send) {
					List<String> sList = new ArrayList<String>(), pList = new ArrayList<String>(), oList = new ArrayList<String>();
					for (Statement st : stmts) {
						String subjectStr = st.getSubject().toString();
						String predicateStr = st.getPredicate().toString();
						String objectStr = st.getObject().toString();
						sList.add(subjectStr);
						pList.add(predicateStr);
						oList.add(objectStr);
						// logger.info(this.getIRI().split("#")[1] + " Streaming: " + q.toString());
					}
					if (this.qosSimulationMode == QosSimulationMode.accuracy) {
						if (this.ed.getInternalQos() != null)
							if (Math.random() >= this.ed.getInternalQos().getAccuracy()) {
								int j = (int) (Math.random() * oList.size());
								while (!oList.get(j).contains("^^")) {
									j = (int) (Math.random() * oList.size());
								}
								double newVehicleCnt = Math.random();
								logger.info("Creating false value: " + newVehicleCnt + ", old value: " + oList.get(j));
								oList.set(j, newVehicleCnt + "^^http://www.w3.org/2001/XMLSchema#double");
							}

					}
					for (int i = 0; i < sList.size(); i++) {
						final RdfQuadruple q = new RdfQuadruple(sList.get(i), pList.get(i), oList.get(i),
								System.currentTimeMillis());
						this.put(q);
					}
				}

			}
		}

	}

	private void sendResultToSession(String[] resultArray) {

		// logger.info("Modifiers: " + jq.getModifierMap());
		// logger.info("Constraints: " + jq.getConstraintMap());
		// logger.info("Operators: " + jq.getOperatorMap());
		try {
			List<JsonQueryResult> results = new ArrayList<JsonQueryResult>();
			JsonQueryResults jqrs = new JsonQueryResults(results);
			for (String requestedProperty : this.requestedProperties) {
				Modifier modifier = this.modifierMap.get(requestedProperty);
				String value = this.constraintMap.get(requestedProperty);
				Operator op = this.operatorMap.get(requestedProperty);
				String abbreviatedPropertyStr = requestedProperty.replaceAll(" ", "").replaceAll("\\.", "");
				logger.debug("Abbrv property: " + abbreviatedPropertyStr);
				if (modifier != null) {
					List<String> values = new ArrayList<String>();
					for (int i = 0; i < this.propertyList.size(); i++) {
						// logger.info("values of "
						if (propertyList.get(i).contains(abbreviatedPropertyStr)) {
							String resultStr = resultArray[i * 2 + 1];
							resultStr = resultStr.replaceAll("\"", "");
							resultStr = resultStr.substring(0, resultStr.indexOf("^"));
							values.add(resultStr);
						}
					}
					logger.debug("values of " + requestedProperty + ": " + values);
					String aggValue = this.getAggregatedValue(modifier, values);
					JsonQueryResult jqr = new JsonQueryResult(requestedProperty, aggValue, modifier,
							this.getPathSegment(), this.isInThreshold(aggValue, op, value));
					results.add(jqr);
				} else {
					for (int i = 0; i < this.propertyList.size(); i++) {
						// logger.info("values of "
						if (propertyList.get(i).contains(abbreviatedPropertyStr)) {
							String resultStr = resultArray[i * 2 + 1];
							resultStr = resultStr.replaceAll("\"", "");
							resultStr = resultStr.substring(0, resultStr.indexOf("^"));
							String foi = this.engine.getRepo().getEds().get(serviceList.get(i)).getFoi();
							JsonQueryResult jqr = new JsonQueryResult(requestedProperty, resultStr, null,
									this.getSegment(foi), this.isInThreshold(resultStr, op, value));
							results.add(jqr);
						}
					}
				}
			}
			String resultStr = new Gson().toJson(jqrs);
			this.session.getBasicRemote().sendText(resultStr);
			logger.info("Sending query results: " + resultStr);
			SubscriberServerEndpoint.startSimulation = true;
		} catch (Exception e) {
			e.printStackTrace();
			// throw e;
			try {
				this.session.getBasicRemote().sendText("FAULT: " + e.getMessage());
			} catch (IOException e1) {
				// TODO Auto-generated catch block
				e1.printStackTrace();
			}
			// this.stop();
			// this.engine.destroy();
			logger.error("Sending error msg: " + e.getMessage());
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
		// System.out.println();
		// System.out.println(this.getIRI() + "-------" + q.size() + " results at SystemTime=["
		// + System.currentTimeMillis() + "]--------");
		// Set<String> currentResults = new HashSet<String>();
		// logger.info(this.getIRI().split("#")[1] + ": " + q.size() + " updates received.");
		for (final RDFTuple t : q) {
			// q.
			// t.g
			// System.out.println(t.toString());
			String result = t.toString().replaceAll("\t", " ").trim();
			if (!this.latestResults.contains(result)) {

				this.latestResults.add(result);
				SubscriptionManagerFactory
						.getSubscriptionManager()
						.getMessageCnt()
						.put(this.getIRI(),
								SubscriptionManagerFactory.getSubscriptionManager().getMessageCnt().get(this.getIRI()) + 1);
				String[] results = result.split(" ");

				sendResultToEngine(results);
				// logger.debug(this.getIRI() + " Streaming: " + result);
			}
		}
		// System.out.println();

	}

	public QosSimulationMode getQosSimulationMode() {
		return qosSimulationMode;
	}

	public void setQosSimulationMode(QosSimulationMode qosSimulationMode) {
		this.qosSimulationMode = qosSimulationMode;
	}

	public int getCorrect() {
		return correct;
	}

	public int getIncorrect() {
		return incorrect;
	}

}
