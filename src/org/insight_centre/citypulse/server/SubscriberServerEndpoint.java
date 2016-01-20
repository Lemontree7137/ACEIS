package org.insight_centre.citypulse.server;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.text.DecimalFormat;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;

import javax.websocket.CloseReason;
import javax.websocket.CloseReason.CloseCodes;
import javax.websocket.OnClose;
import javax.websocket.OnMessage;
import javax.websocket.OnOpen;
import javax.websocket.Session;
import javax.websocket.server.ServerEndpoint;

import org.insight_centre.aceis.engine.ACEISEngine;
import org.insight_centre.aceis.engine.ACEISFactory;
import org.insight_centre.aceis.engine.ACEISScheduler;
import org.insight_centre.aceis.eventmodel.EventDeclaration;
import org.insight_centre.aceis.eventmodel.EventOperator;
import org.insight_centre.aceis.eventmodel.EventPattern;
import org.insight_centre.aceis.eventmodel.OperatorType;
import org.insight_centre.aceis.eventmodel.QosVector;
import org.insight_centre.aceis.eventmodel.Selection;
import org.insight_centre.aceis.eventmodel.WeightVector;
import org.insight_centre.aceis.subscriptions.SubscriptionManagerFactory;
import org.insight_centre.citypulse.commons.data.json.JsonQuery;
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
import org.slf4j.LoggerFactory;

import com.csvreader.CsvReader;
import com.siemens.citypulse.resources.NonFunctionalQualityMetric;

@ServerEndpoint(value = "/subscribe-results")
public class SubscriberServerEndpoint {
	static Map<String, String> idMap = new HashMap<String, String>();
	private static final org.slf4j.Logger logger = LoggerFactory.getLogger(SubscriberServerEndpoint.class);
	public static boolean startSimulation = false;
	static {
		try {
			CsvReader metaData;
			metaData = new CsvReader("dataset/MetaData/trafficMetaData.csv");
			metaData.readHeaders();
			while (metaData.readRecord()) {
				String id = metaData.get("extID");
				String rid = metaData.get("REPORT_ID");
				idMap.put(id, rid);
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
	private final DecimalFormat df = new DecimalFormat("0.00");
	private HashMap<JsonQuery, EventDeclaration> queryMap = new HashMap<JsonQuery, EventDeclaration>();
	private final SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd'T'hh:mm:ss");

	private EventPattern createRequest(JsonQuery jr) {
		EventPattern query = new EventPattern();
		query.setID("EventQuery-" + UUID.randomUUID());
		EventOperator root = new EventOperator(OperatorType.and, 1, query.getID() + "-0");
		query.setQuery(true);
		query.getProvenanceMap().put(root.getID(), new ArrayList<String>());
		query.getEos().add(root);
		// // for (Sensor sensor : jr.getSensors()) {
		// // String eventId = "ED-" + UUID.randomUUID();
		// // EventDeclaration ed = new EventDeclaration(eventId, "", sensor.getType().toString(), null,
		// // new ArrayList<String>(), 5.0);
		// // ed.setServiceId(RDFFileManager.defaultPrefix + ed.getnodeId());
		// // String foiStr = "";
		// // if (!sensor.getType().toString().toLowerCase().contains("weather")) {
		// // double startLat = sensor.getStart().getLat();
		// // double startLon = sensor.getStart().getLon();
		// // double endLat = sensor.getEnd().getLat();
		// // double endLon = sensor.getEnd().getLon();
		// // foiStr = startLat + "," + startLon + "-" + endLat + "," + endLon;
		// //
		// // } else
		// // foiStr = "56.1,10.1-56.1,10.1";
		// ed.setFoi(foiStr);
		// query.getProvenanceMap().get(root.getID()).add(ed.getnodeId());
		List<Selection> sels = new ArrayList<Selection>();

		// for (PropertyType pt : sensor.getObservedProperties()) {
		// // logger.info("adding sensor property.");
		// String pName = "Property-" + UUID.randomUUID();
		// String foiId = "FoI-" + UUID.randomUUID();
		// Selection sel = new Selection(pName, ed.getnodeId(), ed, foiStr, RDFFileManager.ctPrefix
		// + pt.toString());
		// sels.add(sel);
		// ed.getPayloads().add(RDFFileManager.ctPrefix + pt.toString() + "|" + foiId + "|" + pName);
		// }
		// query.getSelections().addAll(sels);
		// query.getEds().add(ed);
		// String serviceType = "";
		// if(sensor.getType())
		// }
		// logger.info("EP query: " + query);
		return query;
	}

	private void deresigterQuery(Session session) {
		// TODO Auto-generated method stub

	}

	@OnClose
	public void onClose(Session session, CloseReason closeReason) throws IOException {
		logger.info(String.format("Session %s closed because of %s", session.getId(), closeReason));
		try {
			this.deresigterQuery(session);
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
				session.close(new CloseReason(CloseCodes.NORMAL_CLOSURE, "Subscriber ended"));
				for (ACEISEngine engine : ACEISScheduler.getAllACEISIntances().values())
					SubscriptionManagerFactory.getSubscriptionManager().deregisterAllEventRequest();
			} catch (Exception e) {
				logger.error("Error occur while stopping streams.");
				e.printStackTrace();
				session.getBasicRemote().sendText("FAULT: " + e.getMessage());
				logger.info("Sending error msg: " + e.getMessage());
				for (ACEISEngine engine : ACEISScheduler.getAllACEISIntances().values()) {
					// SubscriptionManagerFactory.getSubscriptionManager().csparqlEngine.destroy();
					// SubscriptionManagerFactory.getSubscriptionManager().csparqlEngine = new CsparqlEngineImpl();
					// SubscriptionManagerFactory.getSubscriptionManager().csparqlEngine.initialize(true);
				}

				throw new RuntimeException(e);
			}
		} else if (message.contains("stopQuery")) {
			try {
				// ACEISEngine.getSubscriptionManager().deregisterAllEventRequest();
			} catch (Exception e) {
				logger.error("Error occur while stopping streams.");
				e.printStackTrace();
				session.getBasicRemote().sendText("FAULT: " + e.getMessage());
				logger.info("Sending error msg: " + e.getMessage());
				// ACEISEngine.getSubscriptionManager().csparqlEngine.destroy();
				// ACEISEngine.getSubscriptionManager().csparqlEngine = new CsparqlEngineImpl();
				// ACEISEngine.getSubscriptionManager().csparqlEngine.initialize(true);

			}
		} else {
			try {
				logger.info("Handling single query");
				// JsonQuery jr = this.parseMsg(message, session);
				// registerQuery(jr, session);
			} catch (Exception e) {
				// e.printStackTrace();
				session.getBasicRemote().sendText("FAULT: " + e.getMessage());
				logger.info("Sending error msg: " + e.getMessage());
			}
		}
		// Thread.sleep(500);

		return message;
	}

	@OnOpen
	public void onOpen(Session session) {
		logger.info("Subscriber connected ... " + session.getId());
		// sub.setTestSession(session);
	}

	@SuppressWarnings("finally")
	// private JsonQuery parseMsg(String msgStr, Session session) throws Exception {
	// JsonQuery jr = null;
	// // try {
	// Message msg = new Gson().fromJson(msgStr, Message.class);
	// logger.info("Received json message: " + msgStr);
	// jr = new JsonQuery();
	// jr.setId("JQ-" + UUID.randomUUID());
	// jr.setMsg(msg);
	// boolean hasWeather = false, hasAirPollution = false, hasEstimatedTime = false, hasAvgSpeed = false,
	// hasVehicleCount = false, hasTemperature = false, hasHumidity = false, hasWindSpeed = false;
	//
	// for (FunctionalDataset fd : msg.getFunctionalDatasets()) {
	// if (fd.getName().toLowerCase().contains("weather")) {
	// hasWeather = true;
	// List<FunctionalProperty> fps = fd.getProperties();
	// for (FunctionalProperty fp : fps) {
	// if (fp.getName().toLowerCase().contains("temperature"))
	// hasTemperature = true;
	// else if (fp.getName().toLowerCase().contains("humidity"))
	// hasHumidity = true;
	// else if (fp.getName().toLowerCase().contains("windspeed"))
	// hasWindSpeed = true;
	// }
	// if (!hasAvgSpeed && !hasVehicleCount && !hasEstimatedTime)
	// throw new Exception("Must specify at least one weather property.");
	// } else if (fd.getName().toLowerCase().contains("pollution"))
	// hasAirPollution = true;
	// else {
	// List<FunctionalProperty> fps = fd.getProperties();
	// for (FunctionalProperty fp : fps) {
	// if (fp.getName().contains("Avg. Speed"))
	// hasAvgSpeed = true;
	// else if (fp.getName().contains("Vehicle Count"))
	// hasVehicleCount = true;
	// else if (fp.getName().contains("Estimated Time"))
	// hasEstimatedTime = true;
	// }
	// if (!hasAvgSpeed && !hasVehicleCount && !hasEstimatedTime)
	// throw new Exception("Must specify at least one traffic property.");
	// }
	// }
	// // Initialize the userPathGenerator
	// UserPathGenerator userPathGenerator = new UserPathGenerator();
	// // Compute the user's path
	// Location start = new Location(msg.getStartPoint());
	// Location end = new Location(msg.getEndPoint());
	// String transportType = msg.getTransportationType().toString();// "car","bike",
	// // "foot";
	// // current
	// // version
	// // of
	// // GraphHopper only supports for "car"
	// String constraint = "fastest";// other option: "shortest"; current
	// // version of GraphHopper only supports
	// // for
	// // "fastest"
	// userPathGenerator.setParams(start, end, transportType, constraint);
	// Path path = userPathGenerator.generateUserPath();
	// session.getBasicRemote().sendText(new Gson().toJson(path));
	// logger.info("Sending path to client. ");
	// // Compute sensors on path & inflations
	// // SensorsOnPath sensorsOnPath = new SensorsOnPath(path);
	// // List<Sensor> sensors = sensorsOnPath.getSensorsOnPath();
	//
	// if (sensors.size() == 0)
	// throw new Exception("No sensors found on specified path.");
	// else
	// logger.info(sensors.size() + " sensors found on path.");
	// List<Double> inflations = sensorsOnPath.getInflations();
	// for (int i = 0; i < sensors.size(); i++) {
	// jr.getInflationMap().put(sensors.get(i), inflations.get(i));
	// }
	// for (Sensor trafficSensor : sensors) {
	//
	// if (hasEstimatedTime)
	// trafficSensor.addProperty(PropertyType.EstimatedTime);
	// if (hasAvgSpeed)
	// trafficSensor.addProperty(PropertyType.AvgSpeed);
	// if (hasVehicleCount)
	// trafficSensor.addProperty(PropertyType.VehicleCount);
	// logger.debug("Traffic sensor properties: " + trafficSensor.getObservedProperties());
	// }
	// // System.out.println(sensors.toString());
	// // only for fake pollution sensors
	// if (hasAirPollution) {
	// List<Sensor> pollutionSensors = new ArrayList<Sensor>();
	// for (Sensor trafficSensor : sensors) {
	//
	// Sensor pollutionSensor = new Sensor(0, trafficSensor.getStart(), trafficSensor.getStart(), 0.0,
	// SensorType.air_pollution);
	// pollutionSensor.addProperty(PropertyType.API);
	// pollutionSensors.add(pollutionSensor);
	// }
	// sensors.addAll(pollutionSensors);
	// }
	// if (hasWeather) {
	// Sensor weatherSensor = new Sensor(0, start, end, 0.0, SensorType.weather_report);
	// if (hasHumidity)
	// weatherSensor.addProperty(PropertyType.Humidity);
	// if (hasWindSpeed)
	// weatherSensor.addProperty(PropertyType.WindSpeed);
	// if (hasTemperature)
	// weatherSensor.addProperty(PropertyType.Temperature);
	// sensors.add(weatherSensor);
	// }
	//
	// // generate the EventStream
	// EventGenerator eventGenerator = new EventGenerator();
	// List<ContextualEvent> events = eventGenerator.generateEvents(userPathGenerator);
	// eventGenerator.generateEventStream();
	// // generate the UserLocationStream
	// UserStatus userStatus = new UserStatus("user_007", start, ActivityMapping.instance.getActivity(transportType));
	// userPathGenerator.generateUserLocationStream("user_007");
	// jr.setPath(path);
	// jr.setSensors(sensors);
	// jr.setInflationFactors(inflations);
	// jr.setUserStatus(userStatus);
	// logger.info("Enhanced json query: " + new Gson().toJson(jr));
	// return jr;
	//
	// }
	private void registerQuery(JsonQuery jr, Session session) throws Exception {
		EventDeclaration plan = null;
		ACEISEngine engine = ACEISFactory.getACEISInstance();
		try {
			EventPattern queryPattern = this.createRequest(jr);
			// List<EventPattern> queryPatterns = new ArrayList<EventPattern>();
			// queryPatterns.add(queryPattern);
			// RDFFileManager.writeEPsToFile("resultlog/examples/EventRequest.n3", queryPatterns);
			WeightVector weight = new WeightVector(0.0, 0.0, 0.0, 0.0, 0.0, 0.0);
			QosVector constraint = new QosVector(5000, 5000, 1, 0.0, 0.0, 500.0);
			this.retrieveConstAndPref(jr, constraint, weight);
			jr.setConstraint(constraint);
			jr.setWeight(weight);

			plan = SubscriptionManagerFactory.getSubscriptionManager().createCompositionPlan(engine, queryPattern,
					constraint, weight, false, false);
			logger.info("Composition Plan create: " + plan);

			// List<EventDeclaration> plans = new ArrayList<EventDeclaration>();
			// plans.add(plan);
			// RDFFileManager.writeEDsToFile("resultlog/examples/CompositionPlan.n3", plans);
			// System.exit(0); // test only
			this.queryMap.put(jr, plan);
		} catch (Exception e) {
			e.printStackTrace();
			throw e;
		}
		Double freq = (1.0 / 300.0);
		Integer rate = jr.getMsg().getRate();
		if (rate != 0)
			freq = freq * rate;
		plan.getEp().setTimeWindow((int) (1200 / freq));
		Long startDate = jr.getMsg().getStartDate();
		Long stopDate = jr.getMsg().getStopDate();
		logger.info("Setting start and end dates: " + startDate + ", " + stopDate + ". Frequency: " + freq);
		if (startDate != 0 && stopDate != 0)
			SubscriptionManagerFactory.getSubscriptionManager().registerEventRequest(engine, plan, jr, session, freq,
					new Date(jr.getMsg().getStartDate()), new Date(jr.getMsg().getStopDate()), null,
					SubscriptionManagerFactory.getSubscriptionManager().getAdptMode());
		else
			SubscriptionManagerFactory.getSubscriptionManager().registerEventRequest(engine, plan, jr, session, freq,
					null, null, null, null);
		while (!startSimulation) {
			Thread.sleep(500);
		}
		// String ulsStr = "ContextualFilter/Stream/UserLocationService.stream";
		// String esStr = "ContextualFilter/Stream/EventService.stream";
		// ContextualFilteringManager cfManager = new ContextualFilteringManager(jr.getUserStatus(), jr.getPath());
		// ContextualFilteringListener cfl = new ContextualFilteringListener(cfManager);
		// cfl.setSession(session);
		// cfManager.addUpdateListener(cfl);
		// UserLocationStream userLocationStream = new UserLocationStream(cfManager, ulsStr);
		// ACEISEngine.getSubscriptionManager().setUserlocationStream(userLocationStream);
		// if (rate != 0)
		// userLocationStream.setRate(rate / 10);
		// (new Thread(userLocationStream)).start();
		// EventStream eventStream = new EventStream(cfManager, esStr);
		// ACEISEngine.getSubscriptionManager().setEventStream(eventStream);
		// if (rate != 0)
		// eventStream.setRate(rate / 10);
		// (new Thread(eventStream)).start();

	}

	private void retrieveConstAndPref(JsonQuery jq, QosVector constraint, WeightVector weight) {
		for (NonFunctionalQualityMetric nqm : jq.getMsg().getNonFunctionalQualityMetrics()) {
			if (nqm.getName().toLowerCase().equals("latency")) {
				constraint.setLatency(Integer.parseInt(nqm.getValue()));
				if (nqm.getWeightValue() != 0.0)
					weight.setLatencyW(nqm.getWeightValue());
			} else if (nqm.getName().toLowerCase().equals("bandwidth")) {
				constraint.setTraffic(Double.parseDouble(nqm.getValue()));
				if (nqm.getWeightValue() != 0.0)
					weight.setTrafficW(nqm.getWeightValue());
			} else if (nqm.getName().toLowerCase().equals("price")) {
				constraint.setPrice(Integer.parseInt(nqm.getValue()));
				if (nqm.getWeightValue() != 0.0)
					weight.setPriceW(nqm.getWeightValue());
			} else if (nqm.getName().toLowerCase().equals("security")) {
				constraint.setSecurity(Integer.parseInt(nqm.getValue()));
				if (nqm.getWeightValue() != 0.0)
					weight.setSecurityW(nqm.getWeightValue());
			} else if (nqm.getName().toLowerCase().equals("accuracy")) {
				constraint.setAccuracy(Integer.parseInt(nqm.getValue()) / 100.0);
				if (nqm.getWeightValue() != 0.0)
					weight.setAccuracyW(nqm.getWeightValue());
			} else if (nqm.getName().toLowerCase().equals("completeness")) {
				constraint.setReliability(Integer.parseInt(nqm.getValue()) / 100.0);
				if (nqm.getWeightValue() != 0.0)
					weight.setReliabilityW(nqm.getWeightValue());
			}
		}
	}
}
