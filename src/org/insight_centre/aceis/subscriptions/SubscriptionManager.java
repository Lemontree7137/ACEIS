package org.insight_centre.aceis.subscriptions;

import java.io.IOException;
import java.io.PrintWriter;
import java.io.StringWriter;
import java.net.URISyntaxException;
import java.security.KeyManagementException;
import java.security.NoSuchAlgorithmException;
import java.text.ParseException;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.concurrent.ConcurrentHashMap;

import javax.websocket.Session;

//import org.apache.commons.io.IOUtils;
import org.deri.cqels.data.Mapping;
import org.deri.cqels.engine.ContinuousListener;
import org.deri.cqels.engine.ContinuousSelect;
import org.deri.cqels.engine.ExecContext;
import org.deri.cqels.engine.RDFStream;
import org.insight_centre.aceis.engine.ACEISEngine;
import org.insight_centre.aceis.engine.ACEISEngine.RspEngine;
import org.insight_centre.aceis.engine.ACEISScheduler;
import org.insight_centre.aceis.engine.CompositionPlanEnumerator;
import org.insight_centre.aceis.engine.GeneticAlgorithm;
import org.insight_centre.aceis.eventmodel.EventDeclaration;
import org.insight_centre.aceis.eventmodel.EventPattern;
import org.insight_centre.aceis.eventmodel.EventRequest;
import org.insight_centre.aceis.eventmodel.EventRequest.AggregateOperator;
import org.insight_centre.aceis.eventmodel.QosVector;
import org.insight_centre.aceis.eventmodel.Selection;
import org.insight_centre.aceis.eventmodel.TrafficReportService;
import org.insight_centre.aceis.eventmodel.WeightVector;
import org.insight_centre.aceis.io.QosSubscription;
import org.insight_centre.aceis.io.rdf.RDFFileManager;
import org.insight_centre.aceis.io.streams.cqels.CQELSAarhusPollutionStream;
//import org.insight_centre.aceis.io.streams.cqels.AarhusTrafficHttpStream;
import org.insight_centre.aceis.io.streams.cqels.CQELSAarhusTrafficStream;
import org.insight_centre.aceis.io.streams.cqels.CQELSAarhusWeatherStream;
import org.insight_centre.aceis.io.streams.cqels.CQELSLocationStream;
import org.insight_centre.aceis.io.streams.cqels.CQELSResultStream;
import org.insight_centre.aceis.io.streams.cqels.CQELSSensorStream;
import org.insight_centre.aceis.io.streams.cqels.SimpleCQELSResultStream;
import org.insight_centre.aceis.io.streams.cqels.rabbitmq.CQELSAarhusRabbitMQStream;
import org.insight_centre.aceis.io.streams.csparql.CSARQLAarhusPollutionStream;
import org.insight_centre.aceis.io.streams.csparql.CSPARQLAarhusTrafficStream;
import org.insight_centre.aceis.io.streams.csparql.CSPARQLAarhusWeatherStream;
import org.insight_centre.aceis.io.streams.csparql.CSPARQLLocationStream;
import org.insight_centre.aceis.io.streams.csparql.CSPARQLResultObserver;
import org.insight_centre.aceis.io.streams.csparql.CSPARQLSensorStream;
import org.insight_centre.aceis.io.streams.csparql.SimpleCsparqlResultObserver;
import org.insight_centre.aceis.io.streams.csparql.rabbitmq.CSPARQLAarhusRabbitMQStream;
import org.insight_centre.aceis.observations.SensorObservation;
import org.insight_centre.aceis.querytransformation.CQELSQueryTransformer;
import org.insight_centre.aceis.querytransformation.CSPARQLQueryTransformer;
import org.insight_centre.aceis.querytransformation.QueryTransformer;
import org.insight_centre.aceis.querytransformation.TransformationResult;
import org.insight_centre.aceis.subscriptions.TechnicalAdaptationManager.AdaptationMode;
import org.insight_centre.aceis.utils.test.Simulator2.QosSimulationMode;
import org.insight_centre.citypulse.commons.data.json.DataFederationRequest.DataFederationPropertyType;
import org.insight_centre.citypulse.commons.data.json.DataFederationResult;
import org.insight_centre.citypulse.commons.data.json.JsonQuery;
import org.insight_centre.citypulse.server.QosServerEndpoint;
import org.insight_centre.citypulse.server.SubscriberServerEndpoint2;
//import org.insight_centre.contextualfilter.stream.EventStream;
//import org.insight_centre.contextualfilter.stream.UserLocationStream;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.gson.Gson;
import com.hp.hpl.jena.sparql.core.Var;
import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Connection;
import com.rabbitmq.client.ConnectionFactory;

import eu.larkc.csparql.cep.api.RdfStream;
import eu.larkc.csparql.common.RDFTable;
import eu.larkc.csparql.common.RDFTuple;
import eu.larkc.csparql.common.streams.format.GenericObservable;
import eu.larkc.csparql.engine.CsparqlEngine;
import eu.larkc.csparql.engine.CsparqlQueryResultProxy;
import eu.larkc.csparql.engine.RDFStreamFormatter;

/**
 * @author feng
 * 
 */
public class SubscriptionManager {

	private Map<String, TechnicalAdaptationManager> adptMap = new HashMap<String, TechnicalAdaptationManager>();// map
																												// of
																												// TAMs
	private AdaptationMode adptMode;// general/default adaptation mode
	private Map<String, List<EventDeclaration>> compositionMap = new HashMap<String, List<EventDeclaration>>();// map of
																												// composition
																												// plans
																												// for
																												// query
	private Map<String, Long> consumedMessageCnt = new ConcurrentHashMap<String, Long>();
	// private ExecContext context;
	private Map<String, List<RDFStream>> cqelsStreamMap = new HashMap<String, List<RDFStream>>();
	// public CsparqlEngine csparqlEngine;
	// private long receivedMsgCnt = 0;
	private Map<String, CsparqlQueryResultProxy> CsparqlResultList = new HashMap<String, CsparqlQueryResultProxy>();
	private Map<String, RDFStreamFormatter> CsparqlResultObservers = new HashMap<String, RDFStreamFormatter>();
	private Map<String, RdfStream> csparqlStreamMap = new HashMap<String, RdfStream>();

	// private EventStream eventStream = null;
	private Logger logger = LoggerFactory.getLogger(SubscriptionManager.class);
	private Map<String, Integer> messageCnt = new ConcurrentHashMap<String, Integer>();
	private List<Channel> msgBusChannels = new ArrayList<Channel>();
	// public static Map<SensorObservation, Long> latencyMap = new HashMap<SensorObservation, Long>();
	private Map<String, SensorObservation> obMap = new ConcurrentHashMap<String, SensorObservation>();
	private Map<EventDeclaration, ACEISEngine> planEngineMap = new HashMap<EventDeclaration, ACEISEngine>();
	private Map<String, List<String>> qidCompositionPlanmap = new HashMap<String, List<String>>();
	// private Map<String, JsonQuery> qidJqMap = new HashMap<String, JsonQuery>();
	private Map<String, Session> qidSessionMap = new HashMap<String, Session>();
	private Map<String, Date> queryEndTime = new HashMap<String, Date>();
	private ConcurrentHashMap<Session, EventRequest> sessionRequestMap = new ConcurrentHashMap<Session, EventRequest>();
	private Map<String, Date> streamCurrentTimeMap = new HashMap<String, Date>();

	// private Session testSession;
	// private UserLocationStream userlocationStream = null;
	public SubscriptionManager(AdaptationMode adptMode) {
		this.adptMode = adptMode;
	}

	public SubscriptionManager(ExecContext context, CsparqlEngine csparqlEngine) throws Exception {

		// if (context != null && csparqlEngine != null) {
		// throw new Exception("Cannot initialize subscription manager with 2 stream engines");
		// }
		// if (context != null && csparqlEngine == null)
		// this.context = context;
		// else if (csparqlEngine != null && context == null)
		// this.csparqlEngine = csparqlEngine;
		// else
		// this.context = RDFFileManager.initializeCQELSContext("TrafficSensors.n3",
		// ReasonerRegistry.getRDFSReasoner());

	}

	public synchronized void addConsumedMsgCnt(String subscriber) {
		if (this.getConsumedMessageCnt().get(subscriber) == null)
			this.getConsumedMessageCnt().put(subscriber, (long) 0);
		for (Entry<String, Long> e : this.consumedMessageCnt.entrySet()) {
			if (e.getKey().equals(subscriber))
				e.setValue(e.getValue() + 1);
		}
	}

	public EventDeclaration createCompositionPlan(ACEISEngine engine, EventPattern queryPattern, QosVector constraint,
			WeightVector weight, boolean forceRealSensors, boolean useBruteForce) throws Exception {

		engine.getRepo().getReusabilityHierarchy().insertIntoHierarchy(queryPattern);
		// ACEISEngine.getRepo().buildHierarchy();
		CompositionPlanEnumerator cpe = new CompositionPlanEnumerator(engine.getRepo(), queryPattern);
		EventPattern plan = new EventPattern();
		if (useBruteForce)
			plan = cpe.calculateBestFitness(weight, constraint);
		else {
			// for Genetic Algorithms:
			int populationSize = 100;
			int iterations = 100;
			Double mutationRate = 0.05;
			Double factor = 0.3;
			int selectionMode = 1; // 0 for roulette wheels, 1 for ranked sselection
			GeneticAlgorithm ga = new GeneticAlgorithm(cpe, selectionMode, populationSize, iterations, 1.0,
					mutationRate, factor, false, weight, constraint);
			List<EventPattern> results = ga.evolution();
			plan = results.get(results.size() - 1).clone();
			// GA ends
		}
		if (plan.getSize() == 0)
			throw new Exception("Cannot create composition plans under specified constraints.");
		if (forceRealSensors) {
			plan = plan.getCanonicalPattern();
			this.forceRealSensors(plan, engine);
		}
		EventDeclaration compositionPlan = new EventDeclaration(RDFFileManager.defaultPrefix + "CES" + "-"
				+ plan.getID(), RDFFileManager.defaultPrefix + "CES" + "-" + plan.getID(), "complex", plan, null, null);
		compositionPlan.setComposedFor(queryPattern);
		compositionPlan.setServiceId(RDFFileManager.defaultPrefix + "CES" + "-" + plan.getID());
		// ACEISEngine.subscriptionMgr.registerTechnicalAdaptionManager(compositionPlan, constraint, weight);
		// this.analyzeQualityUpdates(compositionPlan);

		return compositionPlan;
	}

	public EventDeclaration createCompositionPlan(ACEISEngine engine, EventPattern queryPattern, QosVector constraint,
			WeightVector weight, boolean forceRealSensors, boolean useBruteForce, int populationSize,
			double crossoverRate, double mutationRate) throws Exception {
		// cpe.setSession(session);
		// EventPattern queryPattern = this.createRequest(jr);
		// WeightVector weight = new WeightVector(0.0, 0.0, 0.0, 0.0, 0.0, 0.0);
		// QosVector constraint = new QosVector(2000, 2000, 1, 0.0, 0.0, 500.0);
		// this.retrieveConstAndPref(jr, constraint, weight);
		//
		// jr.setConstraint(constraint);
		// jr.setWeight(weight);

		logger.info("Constraint Vector: " + constraint.toString());
		logger.info("Weight Vector: " + weight.toString());

		engine.getRepo().getReusabilityHierarchy().insertIntoHierarchy(queryPattern);
		// ACEISEngine.getRepo().buildHierarchy();
		CompositionPlanEnumerator cpe = new CompositionPlanEnumerator(engine.getRepo(), queryPattern);
		EventPattern plan = new EventPattern();
		if (useBruteForce)
			plan = cpe.calculateBestFitness(weight, constraint);
		else {
			// cpe.calculateBestFitness(weight, constraint);
			// for Genetic Algorithms:
			// int populationSize = 100;
			int iterations = 100;
			// Double mutationRate = 0.05;
			Double factor = 0.3;
			int selectionMode = 1; // 0 for roulette wheels, 1 for ranked sselection
			GeneticAlgorithm ga = new GeneticAlgorithm(cpe, selectionMode, populationSize, iterations, crossoverRate,
					mutationRate, factor, false, weight, constraint);
			List<EventPattern> results = ga.evolution();
			plan = results.get(results.size() - 1).clone();
			// GA ends
		}
		if (plan.getSize() == 0)
			throw new Exception("Cannot create composition plans under specified constraints.");
		if (forceRealSensors) {
			plan = plan.getCanonicalPattern();
			this.forceRealSensors(plan, engine);
		}
		EventDeclaration compositionPlan = new EventDeclaration(RDFFileManager.defaultPrefix + "CES" + "-"
				+ plan.getID(), RDFFileManager.defaultPrefix + "CES" + "-" + plan.getID(), "complex", plan, null, null);
		compositionPlan.setComposedFor(queryPattern);
		compositionPlan.setServiceId(RDFFileManager.defaultPrefix + "CES" + "-" + plan.getID());
		// ACEISEngine.subscriptionMgr.registerTechnicalAdaptionManager(compositionPlan, constraint, weight);
		// this.analyzeQualityUpdates(compositionPlan);

		return compositionPlan;
	}

	private void createNstart(ACEISEngine engine, JsonQuery jq, Session session, TransformationResult tr,
			String streamURI, String subscriber, QosSimulationMode mode) throws IOException, ParseException {
		if (engine.getCsparqlEngine() == null) {
			CQELSResultStream rs = new CQELSResultStream(engine.getContext(), streamURI, engine.getContext()
					.registerSelect(tr.getQueryStr()), tr, session, jq);
			// rs.setMessageCnt(messageCnt);
			Thread th = new Thread(rs);
			th.start();
			// th.s
			if (this.cqelsStreamMap.get(engine.getId()) == null)
				this.cqelsStreamMap.put(engine.getId(), new ArrayList<RDFStream>());
			this.cqelsStreamMap.get(engine.getId()).add(rs);
		} else {
			CsparqlQueryResultProxy cqrp = engine.getCsparqlEngine().registerQuery(tr.getQueryStr());
			if (this.messageCnt.get(streamURI) == null)
				this.messageCnt.put(streamURI, 0);
			CSPARQLResultObserver cro = new CSPARQLResultObserver(engine, streamURI, tr, session, jq);
			if (subscriber != null) {
				logger.info("Adding subscriber: " + subscriber + " for: " + streamURI);
				cro.addSubscriber(subscriber);
				if (this.consumedMessageCnt.get(subscriber) == null)
					this.consumedMessageCnt.put(subscriber, (long) 0);
			}
			if (mode != null) {
				cro.setQosSimulationMode(mode);
			}
			logger.info("Registering result observer: " + cro.getIRI());
			engine.getCsparqlEngine().registerStream(cro);
			// RDFStreamFormatter cro = new RDFStreamFormatter(streamURI);
			cqrp.addObserver(cro);
			// String qid = "";
			// this.messageCnt.put(streamURI, 0);
			// for (Entry<String, JsonQuery> e : this.qidJqMap.entrySet()) {
			// if (e.getValue().getId().equals(jq.getId()))
			// qid = e.getKey();
			// }
			// if (!qid.equals(""))
			// this.CsparqlResultList.put(qid, cqrp);
			// else
			// logger.error("could not find qid.");
			this.CsparqlResultList.put(streamURI, cqrp);
			this.CsparqlResultObservers.put(streamURI, cro);
		}
	}

	private void createNstartLocal(ACEISEngine engine, TransformationResult tr, String streamURI)
			throws ParseException, IOException {
		// if (ACEISScheduler.getEngineQueryMap().get(engine.getId()) == null)
		// ACEISScheduler.getEngineQueryMap().put(engine.getId(), new ArrayList<String>());
		ACEISScheduler.getEngineQueryMap().get(engine.getId()).add(streamURI);
		if (engine.getEngineType().equals(RspEngine.CQELS)) {
			SimpleCQELSResultStream rs = new SimpleCQELSResultStream(engine, engine.getContext(), streamURI, tr);
			// rs.setMessageCnt(messageCnt);
			Thread th = new Thread(rs);
			th.start();
			if (this.cqelsStreamMap.get(engine.getId()) == null)
				this.cqelsStreamMap.put(engine.getId(), new ArrayList<RDFStream>());
			this.cqelsStreamMap.get(engine.getId()).add(rs);
			// th.s
			// this.cqelsStreamMap.put(rs.getURI(), rs);
		} else {
			CsparqlQueryResultProxy cqrp = engine.getCsparqlEngine().registerQuery(tr.getQueryStr());
			if (this.messageCnt.get(streamURI) == null)
				this.messageCnt.put(streamURI, 0);
			SimpleCsparqlResultObserver cro = new SimpleCsparqlResultObserver(engine, streamURI, tr);
			// if (subscriber != null) {
			// logger.info("Adding subscriber: " + subscriber + " for: " + streamURI);
			// cro.addSubscriber(subscriber);
			// if (this.consumedMessageCnt.get(subscriber) == null)
			// this.consumedMessageCnt.put(subscriber, (long) 0);
			// }
			// if (mode != null) {
			// cro.setQosSimulationMode(mode);
			// }
			logger.info("Registering result observer: " + cro.getIRI());
			engine.getCsparqlEngine().registerStream(cro);
			// RDFStreamFormatter cro = new RDFStreamFormatter(streamURI);
			cqrp.addObserver(cro);
			// String qid = "";
			// this.messageCnt.put(streamURI, 0);
			// for (Entry<String, JsonQuery> e : this.qidJqMap.entrySet()) {
			// if (e.getValue().getId().equals(jq.getId()))
			// qid = e.getKey();
			// }
			// if (!qid.equals(""))
			// this.CsparqlResultList.put(qid, cqrp);
			// else
			// logger.error("could not find qid.");
			this.CsparqlResultList.put(streamURI, cqrp);
			this.csparqlStreamMap.put(cro.getIRI(), cro);
			this.CsparqlResultObservers.put(streamURI, cro);
		}

	}

	private void createNstartRemote(final ACEISEngine engine, TransformationResult tr, String streamURI)
			throws IOException, ParseException {
		if (engine.getCsparqlEngine() == null) {
			ContinuousSelect sel = engine.getContext().registerSelect(tr.getQueryStr());
			sel.register(new ContinuousListener() {
				@Override
				public void update(Mapping mapping) {
					// String uri = streamURI;
					// InputStream in = IOUtils.toInputStream(result);
					String result = "";
					try {
						for (Iterator<Var> vars = mapping.vars(); vars.hasNext();)
							result += " " + engine.getContext().engine().decode(mapping.get(vars.next()));
					} catch (Exception e) {
						logger.error("CQELS decoding error: " + e.getMessage());
					}

				}
			});
		} else {
			CsparqlQueryResultProxy cqrp = engine.getCsparqlEngine().registerQuery(tr.getQueryStr());
			cqrp.addObserver(new RDFStreamFormatter(streamURI) {
				@Override
				public void update(final GenericObservable<RDFTable> observed, final RDFTable q) {
					for (final RDFTuple t : q) {
						String result = t.toString().replaceAll("\t", " ").trim();
					}
				}
			});
		}
	}

	// public void deregisterEventRequest(Session session) {
	// this.csparqlEngine.unregisterQuery(this.session_query_map.get);
	// }

	private String createNstartRemote(final ACEISEngine engine, TransformationResult tr, final String streamURI,
			final Session session) throws IOException, ParseException {
		final String qid;
		// List<String> serviceList = tr.getServiceList();
		final List<String> pTypeList = tr.getPropertyList();
		// Set<String> uniqueServices = new HashSet<String>();
		// for (String s : serviceList)
		// uniqueServices.add(s);
		// int serviceCnt = uniqueServices.size();
		// int pTypeCnt = pTypeList.size() / serviceCnt;
		// logger.info("Srv List: " + serviceCnt);
		// logger.info("pType List: " + pTypeCnt);
		// logger.info("pType List: " + pTypeList);
		if (engine.getCsparqlEngine() == null) {
			qid = streamURI;
			ContinuousSelect sel = engine.getContext().registerSelect(tr.getQueryStr());
			sel.register(new ContinuousListener() {
				@Override
				public void update(Mapping mapping) {
					logger.info("CQELS results: ");
					// String uri = streamURI;
					// InputStream in = IOUtils.toInputStream(result);
					String result = "";
					try {
						for (Iterator<Var> vars = mapping.vars(); vars.hasNext();)
							result += " " + engine.getContext().engine().decode(mapping.get(vars.next()));
						logger.info("CQELS results: " + result);
						sendResults(result.replaceAll("\t", " ").trim(), pTypeList, engine, session);
					} catch (Exception e) {
						logger.error("CQELS decoding error: " + e.getMessage());
					}
					logger.info("CQELS result: " + result);

				}
			});
			// this.cqelsStreamMap.put(streamURI, sel);
		} else {
			CsparqlQueryResultProxy cqrp = engine.getCsparqlEngine().registerQuery(tr.getQueryStr());
			qid = cqrp.getId();
			cqrp.addObserver(new RDFStreamFormatter(streamURI) {
				// DataFederationPropertyType dfpt = SubscriberServerEndpoint2.query_pType_map.get(streamURI);

				@Override
				public void update(final GenericObservable<RDFTable> observed, final RDFTable q) {
					logger.info("CSPARQL results update. " + q.size());
					for (final RDFTuple t : q) {
						String result = t.toString().replaceAll("\t", " ").trim();
						logger.info("CSPARQL results: " + result);
						sendResults(result, pTypeList, engine, session);
					}
				}
			});
			this.CsparqlResultList.put(streamURI, cqrp);
			// this.CsparqlResultObservers.put(streamURI, cro);
		}
		return qid;
	}

	public void deregisterAllEventRequest() throws Exception {

		// if (this.userlocationStream != null) {
		// this.userlocationStream.stop();
		// this.userlocationStream = null;
		// }
		// SubscriberServerEndpoint.startSimulation = false;
		//
		// if (this.csparqlEngine != null) {
		// for (RdfStream st : this.csparqlStreamMap.values()) {
		// this.csparqlEngine.unregisterStream(st.getIRI());
		// ((CSPARQLSensorStream) st).stop();
		// st = null;
		// }
		// try {
		// for (CsparqlQueryResultProxy cqrp : this.CsparqlResultList.values()) {
		// logger.info("Unregistering query: " + cqrp.getId());
		// this.csparqlEngine.unregisterQuery(cqrp.getId());
		// }
		// this.CsparqlResultList = new HashMap<String, CsparqlQueryResultProxy>();
		// // for (RdfStream st : this.csparqlStreamMap.values())
		// this.csparqlEngine.destroy();
		// this.csparqlEngine = new CsparqlEngineImpl();
		// this.csparqlEngine.initialize(true);
		//
		// } catch (Exception e) {
		// logger.error("Error occured while unregistering csparql query and streams.");
		// e.printStackTrace();
		// this.csparqlEngine.destroy();
		// this.csparqlEngine = new CsparqlEngineImpl();
		// this.csparqlEngine.initialize(true);
		// }
		// this.csparqlStreamMap = new HashMap<String, RdfStream>();
		// } else {
		// for (RDFStream st : this.cqelsStreamMap.values())
		// st.stop();
		// this.cqelsStreamMap = new HashMap<String, RDFStream>();
		// }
	}

	/**
	 * @param plan
	 * 
	 *            deregister the cqels query, stop the result stream
	 */
	public Date deregisterEventRequest(EventDeclaration plan, boolean stopAllStream) {
		Date result = null;
		ACEISEngine engine = this.planEngineMap.get(plan);
		if (engine.getEngineType() == RspEngine.CSPARQL) {
			logger.info("Un-registering query: " + plan.getServiceId());
			engine.getCsparqlEngine().unregisterQuery(this.CsparqlResultList.get(plan.getServiceId()).getId());
			for (Entry<String, Date> e : this.streamCurrentTimeMap.entrySet()) {
				if (result == null || result.before(e.getValue()))
					result = e.getValue();
			}
			if (stopAllStream) {
				for (EventDeclaration ed : plan.getEp().getEds()) {
					this.stopCsparqlStream(ed.getServiceId());
				}
			}
			for (EventDeclaration ed : plan.getEp().getEds())
				if (ed.getEp() != null)
					this.deregisterEventRequest(ed, false);
		} else {

		}
		return result;

	}

	private void forceRealSensors(EventPattern ep, ACEISEngine engine) throws Exception {
		logger.info("Forcing real sensors: " + ep.toString());
		List<String> svcToReplace = new ArrayList<String>();
		for (EventDeclaration ed : ep.getEds()) {
			if (ed.getServiceId().contains("sim"))
				svcToReplace.add(ed.getServiceId());
		}
		for (String s : svcToReplace) {
			ep.replaceED(s, engine.getRepo().getEds().get(RDFFileManager.defaultPrefix + s.split("#")[1].split("-")[0]));
		}
	}

	public AdaptationMode getAdptMode() {
		return adptMode;
	}

	public Map<String, List<EventDeclaration>> getCompositionMap() {
		return compositionMap;
	}

	public Map<String, Long> getConsumedMessageCnt() {
		return consumedMessageCnt;
	}

	public Map<String, RDFStreamFormatter> getCsparqlResultObservers() {
		return CsparqlResultObservers;
	}

	// public ExecContext getContext() {
	// return context;
	// }

	public EventDeclaration getCurrentCompositionPlan(String id) {
		return this.getCompositionMap().get(id).get(0);
	}

	public Map<String, Integer> getMessageCnt() {
		return messageCnt;
	}

	public Map<String, SensorObservation> getObMap() {
		return obMap;
	}

	public Map<String, List<String>> getQidCompositionPlanmap() {
		return qidCompositionPlanmap;
	}

	public Map<String, Session> getQidSessionMap() {
		return qidSessionMap;
	}

	// public Map<String, JsonQuery> getQidJqMap() {
	// return qidJqMap;
	// }

	// private CQELSQueryTransformer transformer = new CQELSQueryTransformer();

	public Map<String, Date> getQueryEndTime() {
		return queryEndTime;
	}

	public ConcurrentHashMap<Session, EventRequest> getSessionRequestMap() {
		// TODO Auto-generated method stub
		return this.sessionRequestMap;
	}

	public Map<String, Date> getStreamCurrentTimeMap() {
		return streamCurrentTimeMap;
	}

	public Map<String, List<RDFStream>> getStreamMap() {
		return cqelsStreamMap;
	}

	/**
	 * subscribe to the qos updates for current composition plan
	 */
	public List<String> initializeQoSUpdates(EventDeclaration plan, Session session, Double freq, Date start, Date end) {
		List<String> sids = new ArrayList<String>();
		for (EventDeclaration ed : plan.getEp().getEds()) {
			sids.add(ed.getServiceId()); // use simulated streams in the begininig
			// sids.add(RDFFileManager.defaultPrefix + ed.getServiceId().split("#")[1].split("-")[0]); // use real
			// sensor
		}
		QosSubscription qs = new QosSubscription(sids, freq, start, end);
		// sids.add(freq + "");
		try {
			session.getBasicRemote().sendText(new Gson().toJson(qs));
		} catch (IOException e) {
			e.printStackTrace();
		}
		return sids;
	}

	private Runnable mockupStream(ACEISEngine engine, String subscriber, Double sensorFreq, String uri, String file,
			EventDeclaration leaf, Date start, Date end, QosSimulationMode mode) {
		if (engine.getEngineType().equals(RspEngine.CQELS)) {
			CQELSSensorStream ds;
			try {
				if (leaf != null && leaf.getEp() != null)
					throw new Exception("Attempting to mock up a non-primitive stream.");
				else {
					ExecContext context = engine.getContext();
					if (leaf instanceof TrafficReportService) {
						ds = new CQELSAarhusTrafficStream(context, uri, "streams/" + file, leaf, start, end);
					} else if (leaf.getEventType().contains("weather"))
						ds = new CQELSAarhusWeatherStream(context, uri, "streams/" + file, leaf, start, end);
					else if (leaf.getEventType().contains("pollution"))
						ds = new CQELSAarhusPollutionStream(context, uri, "streams/" + file, leaf, start, end);
					else
						ds = new CQELSLocationStream(context, uri, "streams/" + file, leaf);
					ds.setRate(sensorFreq);
					if (this.cqelsStreamMap.get(engine.getId()) == null)
						this.cqelsStreamMap.put(engine.getId(), new ArrayList<RDFStream>());
					this.cqelsStreamMap.get(engine.getId()).add(ds);
					// this.cqelsStreamMap.put(ds.getURI(), ds);
					// logger.info("ED payloads: " + leaf.getPayloads());
					new Thread(ds).start();
					return ds;
				}
			} catch (Exception e) {
				// logger.error("")
				e.printStackTrace();
			}
		} else {
			CSPARQLSensorStream ds = null;
			try {
				if (leaf != null && leaf.getEp() != null)
					throw new Exception("Attempting to mock up a non-primitive stream.");
				else {
					if (leaf instanceof TrafficReportService) {
						ds = new CSPARQLAarhusTrafficStream(uri, "streams/" + file, leaf, start, end);
						if (subscriber != null) {
							// logger.info("adding subscriber: " + subscriber + " for: " + file);
							((CSPARQLAarhusTrafficStream) ds).addSubscriber(subscriber);
						}
						if (mode != null) {
							((CSPARQLAarhusTrafficStream) ds).setQosSimulationMode(mode);
						}
					} else if (leaf.getEventType().contains("weather"))
						ds = new CSPARQLAarhusWeatherStream(uri, "streams/" + file, leaf, start, end);
					else if (leaf.getEventType().contains("pollution"))
						ds = new CSARQLAarhusPollutionStream(uri, "streams/" + file, leaf, start, end);
					else
						ds = new CSPARQLLocationStream(uri, "streams/" + file, leaf);
					ds.setRate(sensorFreq);

					this.csparqlStreamMap.put(ds.getIRI(), ds);
					engine.getCsparqlEngine().registerStream(ds);
					// this.streamEndTimeMap.put(ds.getIRI(), end);
					// logger.info("ED payloads: " + leaf.getPayloads());
					new Thread(ds).start();
					return ds;
				}
			} catch (Exception e) {
				// logger.error("")
				e.printStackTrace();
			}
		}

		return null;

	}

	public void registerEventRequest(ACEISEngine engine, EventDeclaration plan, JsonQuery jq, Session session,
			Double sensorFreq, Date start, Date end, String windowStr, AdaptationMode am) throws Exception {
		try {
			String streamURI = "";
			// tracking query related info
			if (plan.getComposedFor() != null) {
				this.queryEndTime.put(plan.getComposedFor().getID(), end);
				// this.qidJqMap.put(plan.getComposedFor().getID(), jq);
				this.qidSessionMap.put(plan.getComposedFor().getID(), session);
			}
			if (plan == null)
				throw new Exception("Registering null composition plan.");
			logger.info("Transforming plan: " + plan.toString());
			logger.info("Pattern: " + plan.getEp());
			streamURI = plan.getServiceId();
			EventPattern ep = plan.getEp();
			logger.info("Loading query string... ");
			QueryTransformer transformer;
			if (engine.getEngineType().equals(RspEngine.CQELS))
				transformer = new CQELSQueryTransformer();
			else
				transformer = new CSPARQLQueryTransformer();
			// queryStr =
			// transformer.loadQueryFromFile("dataset/I2-cqels_query.txt");
			if (windowStr != null)
				transformer.setTimeWindowStr(windowStr);
			TransformationResult transformResult = transformer.transformFromED(plan);
			// queryStr = transformResult.keySet().iterator().next();
			// List<String> serviceList = transformResult.get(queryStr);
			logger.info("Query string loaded: " + transformResult.getQueryStr());

			// Date time = new Date();

			for (EventDeclaration leaf : ep.getEds()) {
				logger.debug("leaf src: " + leaf.getSrc());
				boolean streamExists = true;
				if (engine.getEngineType().equals(RspEngine.CQELS)) {
					if (cqelsStreamMap.get(leaf.getServiceId()) == null)
						streamExists = false;
				} else {
					if (csparqlStreamMap.get(leaf.getServiceId()) == null)
						streamExists = false;
				}

				if (!streamExists)
					if (leaf.getEp() == null) {
						List<Selection> sels = ep.getSelectionOnNode(leaf.getnodeId());
						List<String> requestedProperties = new ArrayList<String>();
						for (Selection sel : sels)
							requestedProperties.add(sel.getPropertyType());
						logger.debug("Requested: " + requestedProperties);
						List<String> payloadsToKeep = new ArrayList<String>();
						logger.debug("payloads: " + leaf.getPayloads());
						for (String payload : leaf.getPayloads()) {
							for (String p : requestedProperties)
								if (payload.contains(p)) {
									payloadsToKeep.add(payload);
									break;
								}
						}
						logger.debug("payloadsToKeep: " + payloadsToKeep);
						leaf.getPayloads().retainAll(payloadsToKeep);
						String fileName = leaf.getServiceId().split("#")[1];
						// temporal workaround for cloned streams
						if (fileName.contains("sim")) {
							fileName = fileName.split("-")[0];
						}
						this.mockupStream(engine, streamURI, sensorFreq, leaf.getServiceId(), fileName + ".stream",
								leaf, start, end, null);
					} else {
						// cnt += 1;
						String timewindowStr = transformer.getTimeWindowStr();
						this.registerEventRequest(engine, leaf, jq, session, sensorFreq, start, end, timewindowStr,
								null);
					}
			}
			createNstart(engine, jq, session, transformResult, streamURI, null, null);
			// this.initializeQoSUpdates(plan, session, sensorFreq, start, end);
			if (am != null) {
				this.registerTechnicalAdaptionManager(plan, jq.getConstraint(), jq.getWeight(), sensorFreq, start, end,
						am);
			}
		} catch (Exception e) {
			e.printStackTrace();
			if (e.getMessage() != null)
				throw e;
			else
				throw new Exception("Failed to register event request.");
		}
	}

	public String registerEventRequestOverMsgBus(ACEISEngine engine, EventDeclaration plan, AdaptationMode am,
			Session session) throws Exception {
		try {
			String streamURI = "";

			if (plan == null)
				throw new Exception("Registering null composition plan.");
			this.planEngineMap.put(plan, engine);
			logger.info("Transforming plan: " + plan.toString());
			logger.info("Pattern: " + plan.getEp());
			streamURI = plan.getServiceId();
			EventPattern ep = plan.getEp();
			logger.info("Loading query string... ");
			QueryTransformer transformer;
			if (engine.getEngineType().equals(RspEngine.CQELS))
				transformer = new CQELSQueryTransformer();
			else
				transformer = new CSPARQLQueryTransformer();
			TransformationResult transformResult = transformer.transformFromED(plan);
			logger.info("Query string loaded: " + transformResult.getQueryStr());

			// Date time = new Date();

			for (EventDeclaration leaf : ep.getEds()) {
				logger.debug("leaf src: " + leaf.getSrc());
				boolean streamExists = true;
				if (engine.getEngineType().equals(RspEngine.CQELS)) {
					List<RDFStream> cqelsStreams = this.cqelsStreamMap.get(engine.getId());
					if (cqelsStreams == null)
						streamExists = false;
					else {
						List<String> streamIds = new ArrayList<String>();
						for (RDFStream st : cqelsStreams) {
							streamIds.add(st.getURI());
						}
						if (!streamIds.contains(leaf.getServiceId()))
							streamExists = false;
					}
					// if (cqelsStreamMap.get(leaf.getServiceId()) == null)
					// streamExists = false;
				} else {
					if (csparqlStreamMap.get(leaf.getServiceId()) == null)
						streamExists = false;
				}

				if (!streamExists) {
					if (leaf.getEp() == null) {
						this.subscribeToMsgBus(engine, leaf);
					} else {
						// cnt += 1;
						String timewindowStr = transformer.getTimeWindowStr();
						this.registerEventRequestOverMsgBus(engine, leaf, am, session);
					}
				} else if (engine.getEngineType().equals(RspEngine.CSPARQL)) {
					engine.getCsparqlEngine().registerStream(csparqlStreamMap.get(leaf.getServiceId()));
				}
				logger.debug("registering existed stream..");
			}
			String qid = createNstartRemote(engine, transformResult, streamURI, session);
			// this.initializeQoSUpdates(plan, session, sensorFreq, start, end);
			// if (am != null) {
			// this.registerTechnicalAdaptionManager(plan, jq.getConstraint(), jq.getWeight(), sensorFreq, start, end,
			// am);
			// }
			return qid;
		} catch (Exception e) {
			e.printStackTrace();
			StringWriter sw = new StringWriter();
			PrintWriter pw = new PrintWriter(sw);
			e.printStackTrace(pw);
			throw new Exception("Failed to register event request. Caused by: " + sw.toString());
		}
	}

	public void registerLocalEventRequest(ACEISEngine engine, EventDeclaration plan, Date start, Date end,
			String windowStr, QosSimulationMode mode) throws Exception {
		try {
			String streamURI = "";
			// tracking query related info
			if (plan.getComposedFor() != null) {
				this.queryEndTime.put(plan.getComposedFor().getID(), end);
				// this.qidJqMap.put(plan.getComposedFor().getID(), jq);
				// this.qidSessionMap.put(plan.getComposedFor().getID(), session);
			}
			if (plan == null)
				throw new Exception("Registering null composition plan.");
			// logger.info("Transforming plan: " + plan.toString()); string
			// logger.info("Pattern: " + plan.getEp());
			streamURI = plan.getServiceId();
			EventPattern ep = plan.getEp();
			// logger.info("Loading query string... ");
			QueryTransformer transformer;
			if (engine.getEngineType().equals(RspEngine.CQELS))
				transformer = new CQELSQueryTransformer();
			else
				transformer = new CSPARQLQueryTransformer();
			if (windowStr != null)
				transformer.setTimeWindowStr(windowStr);
			TransformationResult transformResult = transformer.transformFromED(plan);
			// logger.info("Query string loaded: " + transformResult.getQueryStr());

			for (EventDeclaration leaf : ep.getEds()) {
				logger.debug("leaf src: " + leaf.getSrc());
				boolean streamExists = true;
				if (engine.getEngineType().equals(RspEngine.CQELS)) {
					List<RDFStream> cqelsStreams = this.cqelsStreamMap.get(engine.getId());
					if (cqelsStreams == null)
						streamExists = false;
					else {
						List<String> streamIds = new ArrayList<String>();
						for (RDFStream st : cqelsStreams) {
							streamIds.add(st.getURI());
						}
						if (!streamIds.contains(leaf.getServiceId()))
							streamExists = false;
					}
					// if (cqelsStreamMap.get(leaf.getServiceId()) == null)
					// streamExists = false;
				} else {
					if (csparqlStreamMap.get(leaf.getServiceId()) == null)
						streamExists = false;
				}

				if (!streamExists) {
					if (leaf.getEp() == null) {
						List<Selection> sels = ep.getSelectionOnNode(leaf.getnodeId());
						List<String> requestedProperties = new ArrayList<String>();
						for (Selection sel : sels)
							requestedProperties.add(sel.getPropertyType());
						// logger.info("Requested: " + requestedProperties);
						List<String> payloadsToKeep = new ArrayList<String>();
						// logger.info("payloads: " + leaf.getPayloads());
						for (String payload : leaf.getPayloads()) {
							for (String p : requestedProperties)
								if (payload.contains(p)) {
									payloadsToKeep.add(payload);
									break;
								}
						}
						// logger.info("payloadsToKeep: " + payloadsToKeep);
						leaf.getPayloads().retainAll(payloadsToKeep);
						String fileName = leaf.getEid();
						// temporal workaround for cloned streams
						if (fileName.contains("sim")) {
							fileName = fileName.split("-")[0];
						}
						this.mockupStream(engine, streamURI, leaf.getFrequency(), leaf.getServiceId(), fileName
								+ ".stream", leaf, start, end, mode);
					} else {
						// cnt += 1;
						String timewindowStr = transformer.getTimeWindowStr();
						this.registerLocalEventRequest(engine, leaf, start, end, timewindowStr, mode);
					}
				} else {
					if (engine.getEngineType().equals(RspEngine.CSPARQL)) {
						engine.getCsparqlEngine().registerStream(csparqlStreamMap.get(leaf.getServiceId()));
					}
					logger.debug("registering existed stream..");
				}
			}
			createNstartLocal(engine, transformResult, streamURI);
		} catch (Exception e) {
			e.printStackTrace();
			if (e.getMessage() != null)
				throw e;
			else
				throw new Exception("Failed to register event request.");
		}
	}

	public void registerTechnicalAdaptionManager(EventDeclaration plan, QosVector constraint, WeightVector weight,
			Double freq, Date start, Date end, AdaptationMode am) throws Exception {
		if (this.compositionMap.get(plan.getComposedFor().getID()) == null)
			this.compositionMap.put(plan.getComposedFor().getID(), new ArrayList<EventDeclaration>());
		this.compositionMap.get(plan.getComposedFor().getID()).add(plan);

		if (this.adptMap.get(plan.getComposedFor().getID()) == null) {
			TechnicalAdaptationManager tam = new TechnicalAdaptationManager(this, plan, constraint, weight, freq, am,
					start, end);
			this.adptMap.put(plan.getComposedFor().getID(), tam);
			tam.start(freq);
			// new Thread(tam).start();
		} else {
			// this.adptMap.get(plan.getComposedFor().getID()).update(plan);
		}
	}

	private void removeStream(String uri) {
		// Thread.
		// this.cqelsStreamMap.get(uri).stop();
		// this.cqelsStreamMap.remove(uri);
	}

	private void sendResults(String result, List<String> pTypeList, ACEISEngine engine, Session session) {
		DataFederationResult jsonResult = new DataFederationResult();
		String[] resultArr = result.split(" ");
		int value = 0;
		int cnt = 0;
		List<String> values = new ArrayList<String>();
		for (int i = 1; i < resultArr.length; i += 2) {
			// cnt += 1;
			values.add(resultArr[i]);
			String pType = pTypeList.get((i - 1) / 2);
			if (!jsonResult.getResult().containsKey(pType))
				jsonResult.getResult().put(pType, new ArrayList<String>());
			jsonResult.getResult().get(pType).add(resultArr[i]);

		}
		double resultVal = 0.0;
		AggregateOperator aggOp = engine.getSubscriptionMgr().getSessionRequestMap().get(session).getAggOp();
		if (aggOp == AggregateOperator.avg || aggOp == null) {
			for (Entry<String, ArrayList<String>> en : jsonResult.getResult().entrySet()) {
				double sum = 0.0;
				for (String v : en.getValue()) {
					if (v.contains("."))
						sum += Double.parseDouble(v);
					else
						sum += Integer.parseInt(v) + 0.0;
				}
				sum = sum / en.getValue().size() + 0.0;
				jsonResult.getResult().put(en.getKey(), new ArrayList<String>());
				jsonResult.getResult().get(en.getKey()).add(sum + "");
				logger.debug("adding avg: " + sum);
			}

		} else if (aggOp == AggregateOperator.sum) {
			for (Entry<String, ArrayList<String>> en : jsonResult.getResult().entrySet()) {
				double sum = 0.0;
				for (String v : en.getValue()) {
					if (v.contains("."))
						sum += Double.parseDouble(v);
					else
						sum += Integer.parseInt(v) + 0.0;
				}
				jsonResult.getResult().put(en.getKey(), new ArrayList<String>());
				jsonResult.getResult().get(en.getKey()).add(sum + "");
			}
		} else if (aggOp == AggregateOperator.min) {
			for (Entry<String, ArrayList<String>> en : jsonResult.getResult().entrySet()) {
				double min = 10000000.0;
				for (String v : en.getValue()) {
					double doubleVal = 0.0;
					if (v.contains("."))
						doubleVal = Double.parseDouble(v);
					else
						doubleVal = Integer.parseInt(v) + 0.0;
					if (doubleVal <= min)
						min = doubleVal;
				}
				jsonResult.getResult().put(en.getKey(), new ArrayList<String>());
				jsonResult.getResult().get(en.getKey()).add(min + "");
			}
		} else if (aggOp == AggregateOperator.max)
			for (Entry<String, ArrayList<String>> en : jsonResult.getResult().entrySet()) {
				double max = 0.0;
				for (String v : en.getValue()) {
					double doubleVal = 0.0;
					if (v.contains("."))
						doubleVal = Double.parseDouble(v);
					else
						doubleVal = Integer.parseInt(v) + 0.0;
					if (doubleVal >= max)
						max = doubleVal;
				}
				jsonResult.getResult().put(en.getKey(), new ArrayList<String>());
				jsonResult.getResult().get(en.getKey()).add(max + "");
			}
		try {
			String resultStr = new Gson().toJson(jsonResult);
			logger.info("Sending continous response:" + resultStr);
			session.getBasicRemote().sendText(resultStr);
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}

	public void setAdptMode(AdaptationMode adptMode) {
		this.adptMode = adptMode;
	}

	public void setCompositionMap(Map<String, List<EventDeclaration>> compositionMap) {
		this.compositionMap = compositionMap;
	}

	public void setConsumedMessageCnt(Map<String, Long> consumedMessageCnt) {
		this.consumedMessageCnt = consumedMessageCnt;
	}

	// public void setEventStream(EventStream eventStream) {
	// this.eventStream = eventStream;
	// }

	public void setMessageCnt(Map<String, Integer> messageCnt) {
		this.messageCnt = messageCnt;
	}

	public void setObMap(Map<String, SensorObservation> obMap) {
		this.obMap = obMap;
	}

	public void setQidCompositionPlanmap(Map<String, List<String>> qidCompositionPlanmap) {
		this.qidCompositionPlanmap = qidCompositionPlanmap;
	}

	// public void setQidJqMap(Map<String, JsonQuery> qidJqMap) {
	// this.qidJqMap = qidJqMap;
	// }

	public void setQidSessionMap(Map<String, Session> qidSessionMap) {
		this.qidSessionMap = qidSessionMap;
	}

	public void setQueryEndTime(Map<String, Date> queryEndTime) {
		this.queryEndTime = queryEndTime;
	}

	public void setStreamCurrentTimeMap(Map<String, Date> streamCurrentTimeMap) {
		this.streamCurrentTimeMap = streamCurrentTimeMap;
	}

	// public void setTestSession(Session testSession) {
	// this.testSession = testSession;
	// }

	// public long getReceivedMsgCnt() {
	// return receivedMsgCnt;
	// }

	// public void setReceivedMsgCnt(long receivedMsgCnt) {
	// this.receivedMsgCnt = receivedMsgCnt;
	// }

	// public void setUserlocationStream(UserLocationStream userlocationStream) {
	// this.userlocationStream = userlocationStream;
	// }

	public void stopCsparqlStream(String sid) {
		// logger.info("stopp");
		CSPARQLSensorStream cs = (CSPARQLSensorStream) this.csparqlStreamMap.get(sid);
		cs.stop();
	}

	protected void subscribeToMsgBus(ACEISEngine engine, EventDeclaration ed) throws Exception {
		if (ed.getMsgBusGrounding() == null)
			throw new Exception("Missing msg bus grounding for: " + ed.getServiceId());
		if (engine.getEngineType().equals(RspEngine.CQELS)) {
			try {
				ConnectionFactory factory = new ConnectionFactory();
				String serverAddr = ed.getMsgBusGrounding().getServerAddress();
				Channel channel = null;
				for (Channel c : this.msgBusChannels) {
					if (c.getConnection().getAddress().toString().equals(serverAddr))
						channel = c;
				}
				if (channel == null) {
					factory.setUri(serverAddr);
					Connection conn = factory.newConnection();
					channel = conn.createChannel();
				}
				String exchange = ed.getMsgBusGrounding().getExchange();
				channel.exchangeDeclarePassive(exchange);
				String queueName = channel.queueDeclare().getQueue();

				logger.info("channel opened.");
				CQELSAarhusRabbitMQStream stream = new CQELSAarhusRabbitMQStream(engine.getContext(), channel,
						queueName, ed);
				channel.basicConsume(queueName, false, stream.getURI(), stream);

			} catch (KeyManagementException | NoSuchAlgorithmException | URISyntaxException e) {
				e.printStackTrace();
				throw e;
			} catch (IOException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
				throw e;
			}
		} else if (engine.getEngineType().equals(RspEngine.CSPARQL)) {
			try {
				ConnectionFactory factory = new ConnectionFactory();
				String serverAddr = ed.getMsgBusGrounding().getServerAddress();
				logger.info("server address: " + serverAddr);
				Channel channel = null;
				for (Channel c : this.msgBusChannels) {
					if (c.getConnection().getAddress().toString().equals(serverAddr))
						channel = c;
				}
				if (channel == null) {
					factory.setUri(serverAddr);
					Connection conn = factory.newConnection();
					channel = conn.createChannel();
				}

				String exchange = ed.getMsgBusGrounding().getExchange();
				channel.exchangeDeclarePassive(exchange);
				logger.info("Declaraing exchange: " + exchange);
				String queueName = channel.queueDeclare().getQueue();
				logger.info("channel opened.");
				CSPARQLAarhusRabbitMQStream stream = new CSPARQLAarhusRabbitMQStream(ed.getServiceId(), channel,
						queueName, ed);
				channel.basicConsume(queueName, false, stream.getIRI(), stream);
				engine.getCsparqlEngine().registerStream(stream);

			} catch (KeyManagementException | NoSuchAlgorithmException | URISyntaxException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
				throw e;
			} catch (IOException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
				// e.
				throw e;
			}
		}
	}

	public void subscribeToQosUpdate(EventDeclaration plan, Session session, List<String> sids, Double freq, Date start) {
		logger.info("restarting qos @" + start + ", " + this.getQueryEndTime().get(plan.getComposedFor().getID()));
		QosSubscription qs = new QosSubscription(sids, freq, start, this.getQueryEndTime().get(
				plan.getComposedFor().getID()));
		try {
			session.getBasicRemote().sendText(new Gson().toJson(qs));
		} catch (IOException e) {
			e.printStackTrace();
		}
	}

	// protected void subscribeToStream(int rate, String uri) {
	// AarhusTrafficHttpStream ds = new AarhusTrafficHttpStream(context, uri);
	// ds.setSleep(rate);
	// new Thread(ds).start();
	// this.cqelsStreamMap.put(ds.getURI(), ds);
	// }

	public Date unSubscribeToQosUpdate(Session session, List<String> sids, Double freq) {
		QosSubscription qs = new QosSubscription(sids, freq, null, null);
		Date time = null;
		for (String s : sids) {
			if (time == null || time.after(QosServerEndpoint.getStreamCurrentTimeMap().get(s)))
				time = QosServerEndpoint.getStreamCurrentTimeMap().get(s);
		}
		qs.setUnsubscribe(true);
		try {
			session.getBasicRemote().sendText(new Gson().toJson(qs));
		} catch (IOException e) {
			e.printStackTrace();
		}
		return time;
	}

	// public EventPattern adaptationRequest(String userUniqueEventRequestID,
	// String serviceID) {
	// // TODO Auto-generated method stub
	// return null;
	// }
}
