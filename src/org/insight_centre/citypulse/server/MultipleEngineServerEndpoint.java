package org.insight_centre.citypulse.server;

import java.io.BufferedReader;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.URL;
import java.net.URLConnection;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
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

import org.glassfish.tyrus.server.Server;
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
import org.insight_centre.aceis.io.rdf.VirtuosoDataManager;
import org.insight_centre.aceis.subscriptions.SubscriptionManagerFactory;
import org.insight_centre.aceis.subscriptions.TechnicalAdaptationManager.AdaptationMode;
import org.insight_centre.aceis.utils.test.Simulator2.QosSimulationMode;
import org.insight_centre.citypulse.commons.data.json.Coordinate;
import org.insight_centre.citypulse.commons.data.json.DataFederationRequest;
import org.insight_centre.citypulse.commons.data.json.DataFederationRequest.DataFederationPropertyType;
import org.insight_centre.citypulse.main.MultipleInstanceMain;
import org.slf4j.LoggerFactory;

import com.csvreader.CsvReader;
import com.google.gson.Gson;

@ServerEndpoint(value = "/")
public class MultipleEngineServerEndpoint {
	static Map<String, String> idMap = new HashMap<String, String>();
	public static boolean dummyTest = false;
	private static final org.slf4j.Logger logger = LoggerFactory.getLogger(MultipleEngineServerEndpoint.class);
	public static Map<Session, ACEISEngine> engineMap = new HashMap<Session, ACEISEngine>();
	private static ConcurrentHashMap<Session, EventDeclaration> sessionPlanMap = new ConcurrentHashMap<Session, EventDeclaration>();
	// private static ConcurrentSet<Session> liveSessions;
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

	// private final DecimalFormat df = new DecimalFormat("0.00");
	// private HashMap<JsonQuery, EventDeclaration> queryMap = new HashMap<JsonQuery, EventDeclaration>();
	// private final SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd'T'hh:mm:ss");

	private EventPattern createRequest(DataFederationRequest dfr, List<String> sensors,
			DataFederationPropertyType pType, ACEISEngine engine) throws Exception {
		EventPattern query = new EventPattern();
		query.setID("EventQuery-" + UUID.randomUUID());
		EventOperator root = new EventOperator(OperatorType.and, 1, query.getID() + "-0");
		query.setQuery(true);
		query.getProvenanceMap().put(root.getID(), new ArrayList<String>());
		query.getEos().add(root);
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
		try {
			SubscriptionManagerFactory.getSubscriptionManager().deregisterEventRequest(sessionPlanMap.get(session),
					false);
		} catch (Exception e) {
			session.getBasicRemote().sendText("FAULT: " + e.getMessage());
			logger.info("Sending error msg: " + e.getMessage());
			// ACEISEngine.getSubscriptionManager().csparqlEngine.destroy();
			// ACEISEngine.getSubscriptionManager().csparqlEngine = new CsparqlEngineImpl();
			// ACEISEngine.getSubscriptionManager().csparqlEngine.initialize(true);
		}
	}

	@OnMessage
	public String onMessage(String message, Session session) throws Exception {
		if (message.equals("quit")) {
			try {
				// this.
				session.close(new CloseReason(CloseCodes.NORMAL_CLOSURE, "Subscriber ended"));
				// ACEISEngine.getSubscriptionManager().deregisterAllEventRequest();
				// cachedResult = null;
			} catch (Exception e) {
				logger.error("Error occur while stopping streams.");
				e.printStackTrace();
				session.getBasicRemote().sendText("FAULT: " + e.getMessage());
				logger.info("Sending error msg: " + e.getMessage());
				// SubscriptionManagerFactory.getSubscriptionManager().g.destroy();
				// ACEISEngine.getSubscriptionManager().csparqlEngine = new CsparqlEngineImpl();
				// ACEISEngine.getSubscriptionManager().csparqlEngine.initialize(true);
				// cachedResult = null;
				throw new RuntimeException(e);
			}
		} else if (message.contains("stop")) {
			try {
				ACEISEngine engine = engineMap.get(session);
				SubscriptionManagerFactory.getSubscriptionManager().deregisterEventRequest(sessionPlanMap.get(session),
						false);
				// ACEISEngine engine=SubscriptionManagerFactory.getSubscriptionManager().
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
				// logger.info("Handling single query");
				this.parseMsg(message, session);
				if (!MultipleInstanceMain.getMonitor().isStarted()) {
					new Thread(MultipleInstanceMain.getMonitor()).start();
					logger.info("Monitor thread started.");
				}
				// registerQuery(jr, session);
			} catch (Exception e) {
				e.printStackTrace();
				session.getBasicRemote().sendText("FAULT: " + e.getMessage());
				logger.info("Sending error msg: " + e.getMessage());
			}
		}
		// Thread.sleep(500);

		return null;
	}

	@OnOpen
	public void onOpen(Session session) {
		logger.info("Subscriber connected ... " + session.getId());
		// sub.setTestSession(session);
	}

	@SuppressWarnings("finally")
	private void parseMsg(String msgStr, Session session) throws Exception {
		EventRequest request = new Gson().fromJson(msgStr, EventRequest.class);
		// fixed engine type
		// request.setEngineType(RspEngine.CSPARQL);
		logger.info("Engine type: " + request.getEngineType());
		ACEISEngine engine = ACEISScheduler.getBestEngineInstance(request.getEngineType());
		engineMap.put(session, engine);
		if (request.isContinuous()) {
			this.registerQueryFromEventRequest(request, session);
		} else {
			logger.info("Parsing data fetching request.");
		}

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

	private String getSnapShot(URL url) throws IOException {
		if (dummyTest)
			return (int) (Math.random() * 100) + "";
		else {// test only
			URLConnection yc = url.openConnection();
			BufferedReader in = new BufferedReader(new InputStreamReader(yc.getInputStream()));
			String inputLine;
			String response = "";
			while ((inputLine = in.readLine()) != null)
				response += inputLine;
			in.close();
			return response;
		}
	}

	private List<String> getSensorIds(DataFederationRequest request, ACEISEngine engine) {
		logger.info("Finding sensors on path ...");
		long t1 = System.currentTimeMillis();
		List<String> results = new ArrayList<String>();
		List<EventDeclaration> candidates = new ArrayList<EventDeclaration>();
		List<String> sensorTypes = this.getRelevantSensorTypes(request);
		logger.info("sensor types: " + sensorTypes);
		for (EventDeclaration ed : engine.getRepo().getEds().values()) {
			if (sensorTypes.contains(ed.getEventType()))
				candidates.add(ed);
		}
		logger.info("candidates: " + candidates.size());
		List<Coordinate> route = request.getCoordinates();
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
		logger.info(results.size() + "  sensors found on " + route.size() + "-segment path in " + (t2 - t1) + " ms.");
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
		return results;

	}

	private void registerQuery(DataFederationRequest request, DataFederationPropertyType type, List<String> sensors,
			Session session) throws Exception {
		EventDeclaration plan = null;
		try {
			ACEISEngine bestEngine = engineMap.get(session);
			EventPattern queryPattern = this.createRequest(request, sensors, type, bestEngine);
			WeightVector weight = new WeightVector(0.0, 0.0, 0.0, 0.0, 0.0, 0.0);
			QosVector constraint = new QosVector(5000, 5000, 1, 0.0, 0.0, 500.0);
			if (request.getConstraint() != null)
				constraint = request.getConstraint();
			if (request.getWeight() != null)
				weight = request.getWeight();

			plan = SubscriptionManagerFactory.getSubscriptionManager().createCompositionPlan(bestEngine, queryPattern,
					constraint, weight, false, false);
			logger.info("Composition Plan create: " + plan);
			SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd");
			Date start = sdf.parse("2014-08-01");
			Date end = sdf.parse("2014-08-31");
			SubscriptionManagerFactory.getSubscriptionManager().registerLocalEventRequest(bestEngine, plan, start, end,
					null, QosSimulationMode.none);
		} catch (Exception e) {
			e.printStackTrace();
			throw e;
		}

	}

	private void registerQueryFromEventRequest(EventRequest request, Session session) throws Exception {
		EventDeclaration plan = null;
		try {
			ACEISEngine bestEngine = engineMap.get(session);
			EventPattern queryPattern = request.getEp();

			WeightVector weight = new WeightVector(0.0, 0.0, 0.0, 0.0, 0.0, 0.0);
			QosVector constraint = new QosVector(5000, 5000, 1, 0.0, 0.0, 500.0);
			if (request.getConstraint() != null)
				constraint = request.getConstraint();
			if (request.getWeight() != null)
				weight = request.getWeight();

			plan = SubscriptionManagerFactory.getSubscriptionManager().createCompositionPlan(bestEngine, queryPattern,
					constraint, weight, false, true);
			sessionPlanMap.put(session, plan);
			// logger.info("Composition Plan create: " + plan);
			SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd");
			Date start = sdf.parse("2014-08-01");
			Date end = sdf.parse("2014-08-31");
			// SubscriptionManagerFactory.getSubscriptionManager().registerLocalEventRequest(bestEngine, plan, start,
			// end,
			// null, QosSimulationMode.none);
			SubscriptionManagerFactory.getSubscriptionManager().getSessionRequestMap().put(session, request);
			SubscriptionManagerFactory.getSubscriptionManager().registerEventRequestOverMsgBus(bestEngine, plan, null,
					session);
			AdaptationMode am = SubscriptionManagerFactory.getSubscriptionManager().getAdptMode();
			if (am != null)
				SubscriptionManagerFactory.getSubscriptionManager().registerTechnicalAdaptionManager(plan, constraint,
						weight, null, null, null, am);
		} catch (Exception e) {
			e.printStackTrace();
			throw e;
		}

	}

	public static void main(String[] args) throws Exception {

		// server information
		String hostIP = "127.0.0.1";// args[0];
		int port = 8002;// Integer.parseInt(args[1]);
		HashMap<String, String> parameters = new HashMap<String, String>();
		for (String s : args) {
			parameters.put(s.split("=")[0], s.split("=")[1]);
		}
		if (parameters.get("mode") != null) {
			if (parameters.get("mode").equals("dummy"))
				SubscriberServerEndpoint2.dummyTest = true;
		}

		Server server = new Server(hostIP, port, "/websockets", null, MultipleEngineServerEndpoint.class);
		// Server server2 = new Server(hostIP, port + 1, "/websockets", null, QosServerEndpoint.class);
		try {
			server.start();
			BufferedReader reader = new BufferedReader(new InputStreamReader(System.in));
			System.out.print("Please press a key to stop the server.");
			reader.readLine();
		} catch (Exception e) {
			throw new RuntimeException(e);
		} finally {
			// server.stop();
			// System.exit(0);
			// server2.stop();
		}

	}

}
