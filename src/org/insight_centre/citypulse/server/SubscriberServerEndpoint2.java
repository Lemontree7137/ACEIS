package org.insight_centre.citypulse.server;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;

import javax.websocket.CloseReason;
import javax.websocket.CloseReason.CloseCodes;
import javax.websocket.OnClose;
import javax.websocket.OnMessage;
import javax.websocket.OnOpen;
import javax.websocket.Session;
import javax.websocket.server.ServerEndpoint;

//import org.apache.jena.riot.Lang;
//import org.apache.jena.riot.RDFDataMgr;
import org.insight_centre.aceis.engine.ACEISEngine;
import org.insight_centre.aceis.engine.ACEISEngine.RspEngine;
import org.insight_centre.aceis.engine.ACEISScheduler;
import org.insight_centre.aceis.eventmodel.EventDeclaration;
import org.insight_centre.aceis.eventmodel.EventOperator;
import org.insight_centre.aceis.eventmodel.EventPattern;
import org.insight_centre.aceis.eventmodel.EventRequest;
import org.insight_centre.aceis.eventmodel.OperatorType;
import org.insight_centre.aceis.eventmodel.QosVector;
import org.insight_centre.aceis.eventmodel.Selection;
import org.insight_centre.aceis.eventmodel.WeightVector;
import org.insight_centre.aceis.eventmodel.EventRequest.AggregateOperator;
import org.insight_centre.aceis.io.rdf.VirtuosoDataManager;
import org.insight_centre.aceis.subscriptions.SubscriptionManagerFactory;
import org.insight_centre.citypulse.commons.data.json.Coordinate;
import org.insight_centre.citypulse.commons.data.json.DataFederationRequest;
import org.insight_centre.citypulse.commons.data.json.DataFederationRequest.DataFederationPropertyType;
import org.insight_centre.citypulse.commons.data.json.DataFederationResult;

import com.csvreader.CsvReader;
import com.google.gson.Gson;
import com.hp.hpl.jena.query.QueryExecution;
import com.hp.hpl.jena.query.QueryExecutionFactory;
import com.hp.hpl.jena.query.ResultSet;

//import citypulse.commons.event_request.DataFederationRequest;
//import citypulse.commons.event_request.DataFederationRequest;
//import citypulse.commons.event_request.DataFederationRequest.DataFederationPropertyType;
//import citypulse.commons.event_request.DataFederationResult;
import org.slf4j.LoggerFactory;
//import org.apache.commons.io.IOUtils;
//import org.insight_centre.contextualfilter.data.Path;
//import org.insight_centre.contextualfilter.data.Sensor;
//import org.insight_centre.contextualfilter.data.SensorVariables.PropertyType;
//import org.insight_centre.contextualfilter.data.SensorsOnPath;
//import org.insight_centre.contextualfilter.data.UserStatus;
//import org.insight_centre.contextualfilter.data.SensorVariables.SensorType;
//import org.insight_centre.contextualfilter.engine.ContextualFilteringListener;
//import org.insight_centre.contextualfilter.engine.ContextualFilteringManager;
//import org.insight_centre.contextualfilter.generator.EventGenerator;
//import org.insight_centre.contextualfilter.generator.UserPathGenerator;
//import org.insight_centre.contextualfilter.mapping.ActivityMapping;
//import org.insight_centre.contextualfilter.stream.EventStream;
//import org.insight_centre.contextualfilter.stream.UserLocationStream;
//import org.slf4j.LoggerFactory;

@ServerEndpoint(value = "/")
public class SubscriberServerEndpoint2 {
	static Map<String, String> idMap = new HashMap<String, String>();
	private static Map<Session, ACEISEngine> sessionEngineMap = new HashMap<Session, ACEISEngine>();
	public static boolean dummyTest = false;
	private static final org.slf4j.Logger logger = LoggerFactory.getLogger(SubscriberServerEndpoint2.class);
	private static ConcurrentHashMap<Session, String> session_qid_map = new ConcurrentHashMap<Session, String>();
	public static ConcurrentHashMap<String, DataFederationPropertyType> query_pType_map = new ConcurrentHashMap<String, DataFederationRequest.DataFederationPropertyType>();
	// public static boolean queryInitializing = false;
	// public static DataFederationResult cachedResult = null;

	static {
		try {
			CsvReader metaData;
			metaData = new CsvReader("dataset/MetaData/trafficMetaData.csv");
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
		} catch (FileNotFoundException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}

	}

	private class SimThread implements Runnable {
		private Session session;

		public SimThread(Session session) {
			this.session = session;
		}

		@Override
		public void run() {
			DataFederationResult initSpeed = new DataFederationResult();
			initSpeed.getResult().put(VirtuosoDataManager.ctPrefix + "AverageSpeed", new ArrayList<String>());
			initSpeed.getResult().get(VirtuosoDataManager.ctPrefix + "AverageSpeed")
					.add((int) ((Math.random() * 20) + 30) + "");

			DataFederationResult initAPI = new DataFederationResult();
			initAPI.getResult().put(VirtuosoDataManager.ctPrefix + "AirPollutionIndex", new ArrayList<String>());
			initAPI.getResult().get(VirtuosoDataManager.ctPrefix + "AirPollutionIndex")
					.add((int) ((Math.random() * 50) + 65) + "");
			try {
				logger.info("Sending simulated average speed: " + initSpeed);
				session.getBasicRemote().sendText(new Gson().toJson(initSpeed));
				logger.info("Sending simulated pollution index: " + session.getRequestURI().toString());
				session.getBasicRemote().sendText(new Gson().toJson(initAPI));
			} catch (IOException e1) {
				// TODO Auto-generated catch block
				e1.printStackTrace();
			}
			while (session.isOpen()) {
				try {
					DataFederationResult result = new DataFederationResult();
					result.getResult().put(VirtuosoDataManager.ctPrefix + "AirPollutionIndex", new ArrayList<String>());
					result.getResult().get(VirtuosoDataManager.ctPrefix + "AirPollutionIndex")
							.add((int) ((Math.random() * 50) + 65) + "");
					Thread.sleep(15000);
					logger.info("Sending simulated pollution index: " + result);
					session.getBasicRemote().sendText(new Gson().toJson(result));
				} catch (Exception e) {
					if (e.getMessage().contains("connection has been closed.")) {
						logger.info("client connection closed.");
						// session.close();
					} else
						logger.error(e.getMessage());
				}
			}
			// logger.info("Session closed, deregistering query.");
			// if (sessionEngineMap.get(session).getCsparqlEngine() != null && session_qid_map.get(session) != null)
			// try {
			// logger.info("Un-registering query: " + session_qid_map.get(session));
			// sessionEngineMap.get(session).getCsparqlEngine().unregisterQuery(session_qid_map.get(session));
			// } catch (Exception e) {
			// logger.info("Failed deregister query.");
			// }
			// else
			// logger.info("Closed without deregistering query for the session.");

		}

	}

	// private final DecimalFormat df = new DecimalFormat("0.00");
	// private HashMap<JsonQuery, EventDeclaration> queryMap = new HashMap<JsonQuery, EventDeclaration>();
	// private final SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd'T'hh:mm:ss");

	private EventPattern createRequest(DataFederationRequest dfr, List<String> sensors, DataFederationPropertyType pType)
			throws Exception {
		EventPattern query = new EventPattern();
		query.setID("EventQuery-" + UUID.randomUUID());
		EventOperator root = new EventOperator(OperatorType.and, 1, query.getID() + "-0");
		query.setQuery(true);
		query.getProvenanceMap().put(root.getID(), new ArrayList<String>());
		query.getEos().add(root);
		ACEISEngine engine = ACEISScheduler.getBestEngineInstance(RspEngine.CSPARQL);
		for (String s : sensors) {
			EventDeclaration ed = engine.getRepo().getEds().get(s).clone();
			query.getProvenanceMap().get(root.getID()).add(ed.getnodeId());
			query.getEds().add(ed);
			List<Selection> sels = new ArrayList<Selection>();
			String selectedPropertyType = "";
			if (pType.equals(DataFederationPropertyType.air_quality))
				selectedPropertyType = VirtuosoDataManager.ctPrefix + "API";
			else if (pType.equals(DataFederationPropertyType.average_speed))
				selectedPropertyType = VirtuosoDataManager.ctPrefix + "AverageSpeed";
			else
				throw new Exception("Requested property type not supported.");
			// for (PropertyType pt : sensor.getObservedProperties()) {
			// logger.info("adding sensor property.");
			String pName = "Property-" + UUID.randomUUID();
			// String foiId = "FoI-" + UUID.randomUUID();
			Selection sel = new Selection(pName, ed.getnodeId(), ed, ed.getFoi(), selectedPropertyType);
			sels.add(sel);
			query.getSelections().addAll(sels);
		}
		logger.info("Query: " + query.toString());
		return query;
	}

	// private void deresigterQuery(Session session) throws Exception {
	// ACEISEngine.subscriptionMgr.deregisterAllEventRequest();
	// }

	@OnClose
	public void onClose(Session session, CloseReason closeReason) throws IOException {
		logger.info(String.format("Session %s closed because of %s", session.getId(), closeReason));
		if (sessionEngineMap.get(session).getCsparqlEngine() != null && session_qid_map.get(session) != null)
			try {
				logger.info("Un-registering query: " + session_qid_map.get(session));
				sessionEngineMap.get(session).getCsparqlEngine().unregisterQuery(session_qid_map.get(session));
			} catch (Exception e) {
				logger.info("Failed deregister query.");
			}
		else
			logger.info("Closed without deregistering query for the session.");
		// try {
		// ACEISEngine.getSubscriptionManager().deregisterAllEventRequest();
		// } catch (Exception e) {
		// session.getBasicRemote().sendText("FAULT: " + e.getMessage());
		// logger.info("Sending error msg: " + e.getMessage());
		// ACEISEngine.getSubscriptionManager().csparqlEngine.destroy();
		// ACEISEngine.getSubscriptionManager().csparqlEngine = new CsparqlEngineImpl();
		// ACEISEngine.getSubscriptionManager().csparqlEngine.initialize(true);
		// }
	}

	@OnMessage
	public void onMessage(String message, Session session) throws Exception {
		if (message.equals("quit")) {
			try {
				session.close(new CloseReason(CloseCodes.NORMAL_CLOSURE, "Subscriber ended"));
				// ACEISEngine.getSubscriptionManager().deregisterAllEventRequest();
				// cachedResult = null;
			} catch (Exception e) {
				logger.error("Error occur while stopping streams.");
				e.printStackTrace();
				session.getBasicRemote().sendText("FAULT: " + e.getMessage());
				logger.info("Sending error msg: " + e.getMessage());
				// ACEISEngine.getSubscriptionManager().csparqlEngine.destroy();
				// ACEISEngine.getSubscriptionManager().csparqlEngine = new CsparqlEngineImpl();
				// ACEISEngine.getSubscriptionManager().csparqlEngine.initialize(true);
				// cachedResult = null;
				throw new RuntimeException(e);
			}
		} else if (message.contains("stopQuery")) {
			try {
				// ACEISEngine.getSubscriptionManager().deregisterAllEventRequest();
				// cachedResult = null;
			} catch (Exception e) {
				logger.error("Error occur while stopping streams.");
				e.printStackTrace();
				session.getBasicRemote().sendText("FAULT: " + e.getMessage());
				logger.info("Sending error msg: " + e.getMessage());
				// ACEISEngine.getSubscriptionManager().csparqlEngine.destroy();
				// ACEISEngine.getSubscriptionManager().csparqlEngine = new CsparqlEngineImpl();
				// ACEISEngine.getSubscriptionManager().csparqlEngine.initialize(true);
				// cachedResult = null;

			}
		} else {
			try {
				logger.info("Handling single query");
				this.parseMsg(message, session);
				// registerQuery(jr, session);
			} catch (Exception e) {
				// e.printStackTrace();
				session.getBasicRemote().sendText("FAULT: " + e.getMessage());
				logger.info("Sending error msg: " + e.getMessage());
			}
		}
		// Thread.sleep(500);

		// return message;
	}

	@OnOpen
	public void onOpen(Session session) {
		logger.info("Subscriber connected ... " + session.getId());
		// sub.setTestSession(session);
	}

	@SuppressWarnings("finally")
	private void parseMsg(String msgStr, Session session) throws Exception {
		logger.info("Received json message: " + msgStr);
		DataFederationRequest dfr = new Gson().fromJson(msgStr, DataFederationRequest.class);
		ACEISEngine engine = ACEISScheduler.getBestEngineInstance(RspEngine.CSPARQL);
		logger.info("Engine chosen: " + engine.getId());
		sessionEngineMap.put(session, engine);
		// jr = new JsonQuery();
		// jr.setId("JQ-" + UUID.randomUUID());

		if (dfr.isContinuous()) {
			// if (cachedResult == null && queryInitializing == false) {
			if (!dummyTest) {
				List<String> sids = this.getSensorIds(dfr, engine);
				// queryInitializing = true;
				List<String> trafficSensors = this.getUniqueSensors(sids, "Aarhus_Road_Traffic", engine);
				// List<String> pollutionSensors = this.getUniqueSensors(sids, "Aarhus_Air_Pollution", engine);
				// this.registerQuery(dfr, DataFederationPropertyType.air_quality, pollutionSensors, session);
				this.registerQuery(dfr, DataFederationPropertyType.average_speed, trafficSensors, session);
				SimThread sim = new SimThread(session);
				Thread th = new Thread(sim);
				th.start();
				// while (session.isOpen()) {
				// try {
				// DataFederationResult result = new DataFederationResult();
				// result.getResult().put(VirtuosoDataManager.ctPrefix + "AirPollutionIndex",
				// new ArrayList<String>());
				// result.getResult().get(VirtuosoDataManager.ctPrefix + "AirPollutionIndex")
				// .add((int) (Math.random() * 120) + "");
				// Thread.sleep(15000);
				// logger.info("Sending simulated pollution index: " + result);
				// session.getBasicRemote().sendText(new Gson().toJson(result));
				// } catch (Exception e) {
				// if (e.getMessage().contains("connection has been closed.")) {
				// logger.info("client connection closed.");
				// session.close();
				// } else
				// logger.error(e.getMessage());
				// }
				// }
				// logger.info("Session closed, deregistering query.");
				// if (sessionEngineMap.get(session).getCsparqlEngine() != null && session_qid_map.get(session) != null)
				// try {
				// logger.info("Un-registering query: " + session_qid_map.get(session));
				// sessionEngineMap.get(session).getCsparqlEngine().unregisterQuery(session_qid_map.get(session));
				// } catch (Exception e) {
				// logger.info("Failed deregister query.");
				// }
				// else
				// logger.info("Closed without deregistering query for the session.");
			} else {
				while (true) {
					try {
						DataFederationResult result = new DataFederationResult();
						result.getResult().put(DataFederationPropertyType.average_speed + "", new ArrayList<String>());
						result.getResult().put(DataFederationPropertyType.air_quality + "", new ArrayList<String>());
						result.getResult().put(DataFederationPropertyType.parking_availability + "",
								new ArrayList<String>());
						result.getResult().get(DataFederationPropertyType.average_speed + "")
								.add((int) (Math.random() * 60) + "");
						result.getResult().get(DataFederationPropertyType.air_quality + "")
								.add((int) (Math.random() * 120) + "");
						// result.getResult().get(DataFederationPropertyType.parking_availability)
						// .add((int) (Math.random() * 100) + "");
						session.getBasicRemote().sendText(new Gson().toJson(result));
						Thread.sleep(3000);
					} catch (Exception e) {
						if (e.getMessage().contains("connection has been closed."))
							logger.info("client connection closed.");
						else
							logger.error(e.getMessage());
					}
				}
			}
			// while (cachedResult == null) {
			// try {
			// Thread.sleep(500);
			// } catch (Exception e) {
			// e.printStackTrace();
			// }
			// }
			// // }
			// session.getBasicRemote().sendText(new Gson().toJson(cachedResult));

		} else {
			logger.info("Parsing data fetching request.");
			List<String> sids = this.getUniqueSensors(getSensorIds(dfr, engine), engine);

			DataFederationResult result = new DataFederationResult();

			for (String s : sids) {
				logger.info("sid: " + s);
				EventDeclaration ed = engine.getRepo().getEds().get(s);
				HashMap<String, String> snapshot = VirtuosoDataManager.getSnapShot(ed);
				for (Entry<String, String> en : snapshot.entrySet()) {
					result.getResult().put(en.getKey(), new ArrayList<String>());
					result.getResult().get(en.getKey()).add(en.getValue());
				}
				// String snapShot = this.getSnapShot(ed);
				// String type = ed.getEventType();
				// if (type.toLowerCase().contains("traffic")) {
				// if (result.getResult().get(DataFederationPropertyType.average_speed) == null)
				// result.getResult().put(DataFederationPropertyType.average_speed + "", new ArrayList<String>());
				// result.getResult().get(DataFederationPropertyType.average_speed).add(snapShot);
				// } else if (type.toLowerCase().contains("air")) {
				// if (result.getResult().get(DataFederationPropertyType.air_quality) == null)
				// result.getResult().put(DataFederationPropertyType.air_quality + "", new ArrayList<String>());
				// result.getResult().get(DataFederationPropertyType.air_quality).add(snapShot);
				// } else if (type.toLowerCase().contains("park")) {
				// if (result.getResult().get(DataFederationPropertyType.parking_availability) == null)
				// result.getResult().put(DataFederationPropertyType.parking_availability + "",
				// new ArrayList<String>());
				// result.getResult().get(DataFederationPropertyType.parking_availability).add(snapShot);
				// } else {
				// throw new Exception("Requested snap shot not supported.");
				// }

				// String topic=ed.
			}
			String resultStr = new Gson().toJson(result);
			logger.info("sending response: " + resultStr);
			session.getBasicRemote().sendText(resultStr);
		}
		// return null;

	}

	private List<String> getRelevantSensorTypes(DataFederationRequest dfr) {
		List<DataFederationPropertyType> pTypes = dfr.getPropertyTypes();
		List<String> results = new ArrayList<String>();
		if (pTypes.contains(DataFederationPropertyType.air_quality))
			results.add("Aarhus_Air_Pollution");
		// results.add("Aarhus_Road_Traffic");// FIXME to be fixed
		if (pTypes.contains(DataFederationPropertyType.parking_availability)
				|| pTypes.contains(DataFederationPropertyType.parking_cost))
			results.add("Aarhus_Parking");
		if (pTypes.contains(DataFederationPropertyType.average_speed))
			results.add("Aarhus_Road_Traffic");
		return results;

	}

	private String getSnapShot(EventDeclaration ed) throws Exception {
		if (dummyTest)
			return (int) (Math.random() * 100) + "";
		else {// test only
				// URLConnection yc = url.openConnection();
				// BufferedReader in = new BufferedReader(new InputStreamReader(yc.getInputStream()));
				// String inputLine;
				// String response = "";
				// while ((inputLine = in.readLine()) != null)
				// response += inputLine;
				// in.close();
				//
				// InputStream sin = IOUtils.toInputStream(response);
				// Model m = ModelFactory.createDefaultModel();
				// RDFDataMgr.read(m, sin, Lang.N3);
				// sin.close();
				// String queryStr = VirtuosoDataManager.queryPrefix +
				// " select  ?value where {?obId sao:hasValue ?value}";
				// QueryExecution qe = QueryExecutionFactory.create(queryStr, m);
				// ResultSet results = qe.execSelect();
				// String value = "";
				// if (results.hasNext()) {
				// Literal l = results.next().getLiteral("value");
				// value = l.getString();
				// }
			String value = "";
			String pType = "";
			String sid = ed.getSensorId();
			if (ed.getEventType().equals("Aarhus_Air_Pollution"))
				pType = "AirPollutionLevel";
			else if (ed.getEventType().equals("Aarhus_Road_Traffic"))
				pType = "AverageSpeed";
			else if (ed.getEventType().equals("Aarhus_Parking"))
				pType = "ParkingVacancy";
			else
				throw new Exception("Requested event snapshot unavailable.");
			String queryStr = VirtuosoDataManager.queryPrefix
					+ " select  ?v from <http://iot.ee.surrey.ac.uk/citypulse/datasets/AarhusObservations> where "
					+ "{<"
					+ sid
					+ "> ssn:observes ?p. ?p a <"
					+ VirtuosoDataManager.ctPrefix
					+ pType
					+ "> graph <http://iot.ee.surrey.ac.uk/citypulse/datasets/AarhusObservations> "
					+ "{ ?ob ssn:observedProperty ?p. ?ob sao:value ?v. ?ob sao:time ?t. ?t tl:at ?timestamp. }} order by desc(?timestamp) limit 1";

			String queryStr2 = VirtuosoDataManager.queryPrefix
					+ " select distinct ?pt from <http://iot.ee.surrey.ac.uk/citypulse/datasets/AarhusObservations> where "
					+ "{?sensor ssn:observes ?p. ?p a ?pt. graph <http://iot.ee.surrey.ac.uk/citypulse/datasets/AarhusObservations> "
					+ "{ ?ob ssn:observedProperty ?p. ?ob sao:value ?v. ?ob sao:time ?t. ?t tl:at ?timestamp. }}  ";

			logger.info("Querying virtuoso for snapshot: " + queryStr);
			QueryExecution qe = QueryExecutionFactory.sparqlService(VirtuosoDataManager.virtuosoURI, queryStr,
					VirtuosoDataManager.defaultGraphName);
			ResultSet results = qe.execSelect();
			while (results.hasNext()) {
				value = results.next().get("pt").toString();
				logger.info("snapshot value: " + value);
			}
			// String value=m.getProperty(m.create, arg1)
			return value;
		}
	}

	public static void main(String[] args) {

	}

	private List<String> getSensorIds(DataFederationRequest request, ACEISEngine engine) {

		long t1 = System.currentTimeMillis();
		List<String> results = new ArrayList<String>();
		List<EventDeclaration> candidates = new ArrayList<EventDeclaration>();
		List<String> sensorTypes = this.getRelevantSensorTypes(request);
		// logger.info("sensor types: " + sensorTypes);
		for (EventDeclaration ed : engine.getRepo().getEds().values()) {
			if (sensorTypes.contains(ed.getEventType()))
				candidates.add(ed);
		}
		logger.info("candidates: " + candidates.size());
		List<Coordinate> route = request.getCoordinates();
		logger.info("Finding " + sensorTypes + " sensors on " + route.size() + "-segment path ...");
		for (int i = 1; i < route.size(); i++) {
			// logger.info("route segment: " + i);
			for (EventDeclaration ed : candidates) {
				// logger.info("testing ed: " + ed.getFoi());
				Coordinate start = route.get(i - 1);
				Coordinate end = route.get(i);
				double midLat = ((end.getLatitude() - start.getLatitude()) / 2) + start.getLatitude();
				double midLong = ((end.getLongitude() - start.getLongitude()) / 2) + start.getLongitude();
				double lat = Double.parseDouble(ed.getFoi().split("-")[0].split(",")[0]);
				double longitude = Double.parseDouble(ed.getFoi().split("-")[0].split(",")[1]);
				double radius = ((end.getLatitude() - midLat) * (end.getLatitude() - midLat))
						+ ((end.getLongitude() - midLong) * (end.getLongitude() - midLong));
				// logger.info("testing ed: " + lat + "," + longitude + " mid: " + midLat + "," + midLong);
				if (((lat - midLat) * (lat - midLat) + (longitude - midLong) * (longitude - midLong)) < (1.0 * radius))
					results.add(ed.getServiceId());
			}
		}
		long t2 = System.currentTimeMillis();
		// logger.info(results.size() + "  sensors found on " + route.size() + "-segment path in " + (t2 - t1) +
		// " ms.");
		return results;
	}

	private List<String> getUniqueSensors(List<String> allSensors, String type, ACEISEngine engine) {
		List<String> results = new ArrayList<String>();
		Set<String> fois = new HashSet<String>();
		for (String s : allSensors) {
			EventDeclaration ed = engine.getRepo().getEds().get(s);
			if (ed.getEventType().equals(type)) {
				if (!fois.contains(ed.getFoi())) {
					results.add(s);
					fois.add(ed.getFoi());
				}
			}
		}
		if (results.size() > 10)
			results = results.subList(0, 9);
		logger.info("sensors trimmed: " + results.size());
		logger.info(results.size() + "  sensors found.");
		return results;
	}

	private List<String> getUniqueSensors(List<String> allSensors, ACEISEngine engine) {
		List<String> results = new ArrayList<String>();
		Map<String, List<String>> foiMap = new HashMap<String, List<String>>();
		for (String s : allSensors) {
			EventDeclaration ed = engine.getRepo().getEds().get(s);
			if (foiMap.get(ed.getEventType()) == null) {
				foiMap.put(ed.getEventType(), new ArrayList<String>());
				foiMap.get(ed.getEventType()).add(ed.getFoi());
				results.add(ed.getServiceId());
			} else {
				if (!foiMap.get(ed.getEventType()).contains(ed.getFoi())) {
					foiMap.get(ed.getEventType()).add(ed.getFoi());
					results.add(ed.getServiceId());
				}
			}
		}
		logger.info(results.size() + "  sensors found.");
		return results;

	}

	private void registerQuery(DataFederationRequest request, DataFederationPropertyType type, List<String> sensors,
			Session session) throws Exception {
		EventDeclaration plan = null;
		try {
			EventPattern queryPattern = this.createRequest(request, sensors, type);
			WeightVector weight = new WeightVector(1.0, 1.0, 1.0, 1.0, 1.0, 1.0);
			QosVector constraint = new QosVector(3000, 3000, 1, 0.0, 0.0, 500.0);
			if (request.getConstraint() != null)
				constraint = request.getConstraint();
			// if (request.getWeight() != null)
			// weight = request.getWeight();
			ACEISEngine engine = sessionEngineMap.get(session);
			EventRequest er = new EventRequest(null, constraint, weight);
			er.setAggOp(AggregateOperator.avg);
			er.setContinuous(true);
			er.setEngineType(RspEngine.CSPARQL);
			// er.s
			// er.setAggOp(AggregationOperator.);
			plan = SubscriptionManagerFactory.getSubscriptionManager().createCompositionPlan(engine, queryPattern,
					constraint, weight, false, false);
			logger.info("Composition Plan create: " + plan);
			SubscriptionManagerFactory.getSubscriptionManager().getSessionRequestMap().put(session, er);
			query_pType_map.put(plan.getServiceId(), type);
			String qid = SubscriptionManagerFactory.getSubscriptionManager().registerEventRequestOverMsgBus(engine,
					plan, null, session);
			logger.info("Adding query: " + qid);
			session_qid_map.put(session, qid);

			// this.registerResultListener(tr, plan.getServiceId(), null, mode);
			// List<EventDeclaration> plans = new ArrayList<EventDeclaration>();
			// plans.add(plan);
			// RDFFileManager.writeEDsToFile("resultlog/examples/CompositionPlan.n3", plans);
			// System.exit(0); // test only
			// this.queryMap.put(jr, plan);
		} catch (Exception e) {
			e.printStackTrace();
			throw e;
		}
		// Double freq = (1.0 / 300.0);
		// // Integer rate = jr.getMsg().getRate();
		// // if (rate != 0)
		// // freq = freq * rate;
		// plan.getEp().setTimeWindow((int) (1200 / freq));

	}
	// private void registerResultListener(TransformationResult tr, String streamURI, String subscriber,
	// QosSimulationMode mode) throws IOException, ParseException {
	// if (this.csparqlEngine == null) {
	// CQELSResultStream rs = new CQELSResultStream(context, streamURI, context.registerSelect(tr.getQueryStr()),
	// tr, session, jq);
	// // rs.setMessageCnt(messageCnt);
	// Thread th = new Thread(rs);
	// th.start();
	// // th.s
	// this.cqelsStreamMap.put(rs.getURI(), rs);
	// } else {
	// CsparqlQueryResultProxy cqrp = this.csparqlEngine.registerQuery(tr.getQueryStr());
	// // if (this.messageCnt.get(streamURI) == null)
	// // this.messageCnt.put(streamURI, 0);
	// CSPARQLResultObserver cro = new CSPARQLResultObserver(ACEISEngine.csparqlEngine, streamURI, tr, session, jq);
	// if (subscriber != null) {
	// logger.info("Adding subscriber: " + subscriber + " for: " + streamURI);
	// cro.addSubscriber(subscriber);
	// if (this.consumedMessageCnt.get(subscriber) == null)
	// this.consumedMessageCnt.put(subscriber, (long) 0);
	// }
	// if (mode != null) {
	// cro.setQosSimulationMode(mode);
	// }
	// logger.info("Registering result observer: " + cro.getIRI());
	// csparqlEngine.registerStream(cro);
	// // RDFStreamFormatter cro = new RDFStreamFormatter(streamURI);
	// cqrp.addObserver(cro);
	// // String qid = "";
	// // this.messageCnt.put(streamURI, 0);
	// // for (Entry<String, JsonQuery> e : this.qidJqMap.entrySet()) {
	// // if (e.getValue().getId().equals(jq.getId()))
	// // qid = e.getKey();
	// // }
	// // if (!qid.equals(""))
	// // this.CsparqlResultList.put(qid, cqrp);
	// // else
	// // logger.error("could not find qid.");
	// this.CsparqlResultList.put(streamURI, cqrp);
	// this.CsparqlResultObservers.put(streamURI, cro);
	// }
	// }

}
