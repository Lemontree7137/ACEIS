package org.insight_centre.aceis.subscriptions;

import java.io.FileWriter;
import java.net.URI;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.TreeMap;
import java.util.UUID;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import javax.websocket.ClientEndpoint;
import javax.websocket.CloseReason;
import javax.websocket.OnClose;
import javax.websocket.OnMessage;
import javax.websocket.OnOpen;
import javax.websocket.Session;

import org.glassfish.tyrus.client.ClientManager;
import org.insight_centre.aceis.engine.ACEISEngine;
import org.insight_centre.aceis.engine.ACEISFactory;
import org.insight_centre.aceis.engine.CompositionPlanEnumerator;
import org.insight_centre.aceis.eventmodel.EventDeclaration;
import org.insight_centre.aceis.eventmodel.EventPattern;
import org.insight_centre.aceis.eventmodel.NodeRemovalException;
import org.insight_centre.aceis.eventmodel.QosVector;
import org.insight_centre.aceis.eventmodel.WeightVector;
import org.insight_centre.aceis.io.rdf.RDFFileManager;
import org.insight_centre.aceis.observations.QosUpdate;
import org.insight_centre.citypulse.server.QosServerEndpoint;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.csvreader.CsvWriter;
import com.google.gson.Gson;

@ClientEndpoint
public class TechnicalAdaptationManager {
	public enum AdaptationMode {
		global, incremental, local, na;
	}
	private class AdpatationMonitor implements Runnable {
		private CsvWriter cw;
		private ACEISEngine engine;
		private final Logger logger = LoggerFactory.getLogger(AdpatationMonitor.class);
		private String qid;
		private SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss");

		public AdpatationMonitor(String qid, ACEISEngine engine) {
			super();
			this.qid = qid;
			this.engine = engine;
		}

		private void printResults() {
			// print results
			long resultCnt = SubscriptionManagerFactory.getSubscriptionManager().getConsumedMessageCnt().get(qid);
			// long updateCnt1 = updateCnt;
			int criticalUpdates = adaptations.size();
			int successfulAdpt = 0;
			long delaySum = 0;
			for (AdaptationResult ar : adaptations) {
				delaySum += ar.getTime();
				if (ar.isSucess())
					successfulAdpt += 1;
			}
			long violated = 0;
			long total = 0;
			for (Entry<Date, List<QosVector>> e : qosMap.entrySet()) {
				for (QosVector q : e.getValue())
					if (q.getAccuracy() < constraint.getAccuracy())
						violated++;
				total += e.getValue().size();
			}
			double avgDelay = 0.0;
			if (adaptations.size() > 0)
				avgDelay = delaySum / adaptations.size();
			try {
				cw = new CsvWriter(new FileWriter("resultLog/adpt/" + qid + ".csv", true), ',');
				cw.write("result_cnt");
				cw.write("update_rcv");
				cw.write("update_sent");
				cw.write("crit_update_cnt");
				cw.write("avg_adpt_time");
				cw.write("success_adpt_cnt");
				cw.write("total_updates");
				cw.write("violated_updates");
				cw.write("localReplacement");
				cw.write("localRecompose");
				cw.write("parentReplacement");
				cw.write("globalRecompose");
				cw.endRecord();
				cw.write(resultCnt + "");
				cw.write(updateCnt + "");
				cw.write(QosServerEndpoint.updateCnt + "");
				cw.write(criticalUpdates + "");
				cw.write(avgDelay + "");
				cw.write(successfulAdpt + "");
				cw.write(total + "");
				cw.write(violated + "");
				cw.write(localReplacement + "");
				cw.write(localRecompose + "");
				cw.write(parentReplacement + "");
				cw.write(globalRecompose + "");
				cw.endRecord();
				cw.write(" ");
				cw.write("acc");
				cw.write("agg_from");
				cw.endRecord();
				Date start = null;
				ArrayList<QosVector> qosList = new ArrayList<QosVector>();

				for (Entry<Date, List<QosVector>> e : qosMap.entrySet()) {
					if (start == null)
						start = e.getKey();
					if (TimeUnit.MILLISECONDS.toMinutes(e.getKey().getTime() - start.getTime()) >= 55) {
						cw.write(sdf.format(e.getKey()).split("T")[1]);
						Double avgAcc = 0.0;
						for (QosVector qos : qosList)
							avgAcc += qos.getAccuracy();
						cw.write(avgAcc / qosList.size() + "");
						cw.write(qosList.size() + "");
						cw.endRecord();
						start = null;
						qosList = new ArrayList<QosVector>();
					} else {
						qosList.addAll(e.getValue());
					}
				}
				cw.close();
			} catch (Exception e) {
				e.printStackTrace();
			}
		}

		@Override
		public void run() {
			while (liveQosStreams.size() > 3) {
				try {
					logger.info("remaining live qos streams: " + liveQosStreams.size()
							+ ", current received csparql msgs: " + sub.getConsumedMessageCnt().get(qid));
					Thread.sleep(5000);
				} catch (InterruptedException e) {
					e.printStackTrace();
				}
			}
			this.printResults();
			System.exit(0);
		}
	}

	private class QosMonitor implements Runnable {
		private Map<String, QosVector> epQosMap = new HashMap<String, QosVector>();
		private final Logger logger = LoggerFactory.getLogger(QosMonitor.class);

		public QosMonitor() {
			super();
			for (EventPattern ep : engine.getRepo().getEps().values()) {
				try {
					QosVector orginalQos = ep.aggregateQos();
					this.epQosMap.put(ep.getID(), orginalQos);
					logger.debug("Putting: " + orginalQos + " for: " + ep.getID());
				} catch (CloneNotSupportedException | NodeRemovalException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}
			}
		}

		@Override
		public void run() {
			while (true) {
				try {
					boolean changed = false;
					for (EventPattern ep : engine.getRepo().getEps().values()) {
						// logger.info("Checking qos: " + ep.getID());
						QosVector currentQos = ep.aggregateQos();
						if (!currentQos.equals(this.epQosMap.get(ep.getID()))) {
							logger.debug("CES qos update: " + ep.getID());
							this.epQosMap.put(ep.getID(), currentQos);
							if (!changed)
								changed = true;
						}
					}
					// logger.info("No qos changes affected CES.");
					Thread.sleep(500);
				} catch (InterruptedException e) {
					e.printStackTrace();
				} catch (CloneNotSupportedException e) {
					e.printStackTrace();
				} catch (NodeRemovalException e) {
					e.printStackTrace();
				}
			}

		}
	}

	// public enum IncrementalSuccessMode {
	// localReplacement, localRecompose, parentReplacement, globalRecompose
	// }

	private boolean adaptationLock = false;

	private List<AdaptationResult> adaptations = new ArrayList<AdaptationResult>();

	ClientManager client = ClientManager.createClient();
	private QosVector constraint;
	private EventDeclaration currentCompositionPlan;
	private ACEISEngine engine;
	private Double freq;
	private CountDownLatch latch;
	private HashMap<String, QosVector> latestQosMap = new HashMap<String, QosVector>();
	private List<String> liveQosStreams = new ArrayList<String>();
	private int localReplacement, localRecompose, parentReplacement, globalRecompose = 0;
	private Logger logger = LoggerFactory.getLogger(this.getClass());
	private AdaptationMode mode;
	private TreeMap<Date, List<QosVector>> qosMap = new TreeMap<Date, List<QosVector>>();
	private Date start, end;
	// private EventPattern query;
	private SubscriptionManager sub;
	private long updateCnt = 0;
	final String uri = "ws://localhost:8081/websockets/qos";
	private WeightVector weight;

	public TechnicalAdaptationManager(SubscriptionManager sub, EventDeclaration currentCompositionPlan,
			QosVector constraint, WeightVector weight, Double freq, AdaptationMode mode, Date start, Date end) {
		super();
		this.sub = sub;
		this.currentCompositionPlan = currentCompositionPlan;
		this.constraint = constraint;
		this.weight = weight;
		this.freq = freq;
		this.mode = mode;
		this.start = start;
		this.end = end;
		// this.forceRealSensors(currentCompositionPlan);
	}

	/**
	 * @param serviceID
	 * @return new composition plan conforming with user's constraints
	 * @throws Exception
	 */
	protected EventDeclaration adaptation(String serviceID) throws Exception {
		// adaptationLock = true;
		logger.info("identifying service: " + serviceID + " current plan: " + this.currentCompositionPlan.getEp());
		serviceID = this.identifyLocalServiceID(serviceID);
		logger.info("identified service: " + serviceID);
		EventDeclaration result = null;
		if (this.mode == AdaptationMode.local) {
			EventPattern newPattern = currentCompositionPlan.getEp().clone();
			CompositionPlanEnumerator cpe = new CompositionPlanEnumerator(engine.getRepo(), newPattern);
			for (EventDeclaration ed : currentCompositionPlan.getEp().getEds()) {
				if (ed.getServiceId().equals(serviceID)) {
					EventDeclaration newEd = cpe.getBestSubsitituteEDforED(ed, constraint, weight);
					if (newEd != null) {
						// if(newEd)
						newPattern.replaceED(serviceID, newEd);
						if (newPattern.aggregateQos().satisfyConstraint(constraint)) {
							result = this.currentCompositionPlan.clone();
							result.setEp(newPattern);
							result.setComposedFor(this.currentCompositionPlan.getComposedFor());
							break;
						} else {
							logger.info("Adaptation failed. Mode: " + this.mode);
							// logger.info("====== qos details ======");
							// logger.info("old: " + currentCompositionPlan.getEp().aggregateQos());
							// for (EventDeclaration ed3 : currentCompositionPlan.getEp().getEds()) {
							// logger.info(ed3.getServiceId().split("#")[1] + ": " + ed3.getExternalQos());
							// }
							// logger.info("-------------------------");
							// logger.info("new: " + newPattern.aggregateQos());
							// for (EventDeclaration ed2 : newPattern.getEds()) {
							// logger.info(ed2.getServiceId().split("#")[1] + ": " + ed2.getExternalQos());
							// }
						}
					}
				}
			}
		} else if (this.mode == AdaptationMode.global) {
			EventDeclaration newCompositionPlan = SubscriptionManagerFactory.getSubscriptionManager()
					.createCompositionPlan(engine, this.currentCompositionPlan.getComposedFor(), constraint, weight,
							false, false);
			if (newCompositionPlan.getEp().aggregateQos().satisfyConstraint(constraint)) {
				result = newCompositionPlan;
				result.getEp().setTimeWindow(this.currentCompositionPlan.getEp().getTimeWindow());
				// result.setEp(newPattern);
				// result.setComposedFor(this.currentCompositionPlan.getComposedFor());
			} else {
				logger.info("Adaptation failed. Mode: " + this.mode);
			}
		} else if (this.mode == AdaptationMode.incremental) {
			logger.info("Starting Incremental Adaptation for: " + serviceID);
			boolean adapted = false;
			EventPattern newPattern = currentCompositionPlan.getEp().clone();
			CompositionPlanEnumerator cpe = new CompositionPlanEnumerator(engine.getRepo(), newPattern);
			for (EventDeclaration ed : currentCompositionPlan.getEp().getEds()) {
				if (ed.getServiceId().equals(serviceID)) {
					EventDeclaration newEd = cpe.getBestSubsitituteEDforED(ed, constraint, weight);
					if (newEd != null) { // try to replace this service
						// if(newEd)
						newPattern.replaceED(serviceID, newEd);
						if (newPattern.aggregateQos().satisfyConstraint(constraint)) {
							result = this.currentCompositionPlan.clone();
							result.setEp(newPattern);
							result.setComposedFor(this.currentCompositionPlan.getComposedFor());
							adapted = true;
							logger.info("Local replacement sucess. Mode: " + this.mode);
							localReplacement += 1;
							break;
						} else {
							logger.info("Local replacement failed. Mode: " + this.mode);
						}
					}
					if (!adapted) {// try to re-compose this service
						newPattern = currentCompositionPlan.getEp().clone();
						EventPattern subPattern = currentCompositionPlan.getEp().getSubtreeByRoot(serviceID).clone();
						if (subPattern.getSize() > 1) {
							EventDeclaration newSubCompositionPlan;
							if (subPattern.getSize() > 4) // GA suitable for large sub-patterns only
								newSubCompositionPlan = SubscriptionManagerFactory.getSubscriptionManager()
										.createCompositionPlan(engine, subPattern, constraint, weight, false, false);
							else
								newSubCompositionPlan = this.sub.createCompositionPlan(engine, subPattern, constraint,
										weight, false, true);
							newPattern.replaceSubtree(serviceID, newSubCompositionPlan.getEp());
							if (newPattern.aggregateQos().satisfyConstraint(constraint)) {
								result = this.currentCompositionPlan.clone();
								result.setEp(newPattern);
								result.setComposedFor(this.currentCompositionPlan.getComposedFor());
								adapted = true;
								logger.info("Local re-composition sucess. Mode: " + this.mode);
								localRecompose += 1;
							} else {
								logger.info("Local re-composition failed - constraint not satisfied. Mode: "
										+ this.mode);
							}
						} else {
							logger.info("Local re-composition failed - subPattern size = 1. Mode: " + this.mode);
						}
					}
					if (!adapted) { // try to replace parent services

						List<String> candidates = engine.getRepo().getReusabilityHierarchy()
								.getAllDescendants(this.currentCompositionPlan.getComposedFor().getID());
						List<EventDeclaration> eds = new ArrayList<EventDeclaration>();
						for (String s : candidates)
							eds.add(engine.getRepo().getEDByEPId(s));
						logger.info("Parent replacements found: " + eds.size());
						List<EventDeclaration> sortedEds = this.getSortedEdByEpSize(eds);
						for (EventDeclaration candidate : sortedEds) {
							newPattern = currentCompositionPlan.getEp().clone();
							newPattern.replaceAndSubPattern(newPattern.getRootId(), candidate);
							if (newPattern.aggregateQos().satisfyConstraint(constraint)) {
								result = this.currentCompositionPlan.clone();
								result.setEp(newPattern);
								result.setComposedFor(this.currentCompositionPlan.getComposedFor());
								adapted = true;
								logger.info("Local parent replacement sucess: " + candidate.toString() + " Mode: "
										+ this.mode);
								parentReplacement += 1;
								break;
							}
						}
						if (!adapted) {
							logger.info("Local parent replacement failed - constraint not satisfied. Mode: "
									+ this.mode);
						}
					}
					if (!adapted) { // try global adaptation
						EventDeclaration newCompositionPlan = SubscriptionManagerFactory.getSubscriptionManager()
								.createCompositionPlan(engine, this.currentCompositionPlan.getComposedFor(),
										constraint, weight, false, false);
						if (newCompositionPlan.getEp().aggregateQos().satisfyConstraint(constraint)) {
							result = newCompositionPlan;
							result.getEp().setTimeWindow(this.currentCompositionPlan.getEp().getTimeWindow());
							globalRecompose += 1;
						} else {
							logger.info("Global Adaptation failed. Mode: " + this.mode);
						}
					}
				}
			}
		}
		return result;
	}

	private void findDifferences(EventDeclaration newPlan, List<String> streamsToRemove, List<String> streamsToAdd)
			throws CloneNotSupportedException {
		List<String> oldSids = new ArrayList<String>();
		for (EventDeclaration ed : this.currentCompositionPlan.getEp().clone().getCompletePattern().getEds()) {
			oldSids.add(ed.getServiceId());
		}
		List<String> newSids = new ArrayList<String>();
		for (EventDeclaration ed : newPlan.getEp().clone().getCompletePattern().getEds()) {
			newSids.add(ed.getServiceId());
		}
		for (String s : oldSids) {
			if (!newSids.contains(s)) {
				streamsToRemove.add(s);
			}
		}
		for (String s : newSids) {
			if (!oldSids.contains(s)) {
				streamsToAdd.add(s);
			}
		}
		logger.info("Streams to remove: " + streamsToRemove);
		logger.info("Streams to add: " + streamsToAdd);
		this.liveQosStreams.addAll(streamsToAdd);
		this.liveQosStreams.removeAll(streamsToRemove);
	}

	private void forceRealSensors(EventDeclaration plan) {
		for (EventDeclaration ed : plan.getEp().getEds()) {
			ed.setServiceId(RDFFileManager.defaultPrefix + ed.getServiceId().split("#")[1].split("-")[0]);
		}
	}

	public QosVector getConstraint() {
		return constraint;
	}

	private EventDeclaration getEDByEPId(String epId, List<EventDeclaration> eds) {
		for (EventDeclaration ed : eds) {
			if (ed.getEp().getID().equals(epId))
				return ed;
		}
		return null;
	}

	public AdaptationMode getMode() {
		return mode;
	}

	private List<EventDeclaration> getSortedEdByEpSize(List<EventDeclaration> eds) throws Exception {
		List<EventDeclaration> results = new ArrayList<EventDeclaration>();
		List<EventPattern> eps = new ArrayList<EventPattern>();
		int maxCnt = 0;
		for (EventDeclaration ed : eds) {
			if (ed.getEp() != null) {
				eps.add(ed.getEp());
			} else
				throw new Exception("Sorting EDs with null EP.");
		}
		Collections.sort(eps);
		for (EventPattern ep : eps) {
			results.add(this.getEDByEPId(ep.getID(), eds));
		}
		return results;
	}

	public long getUpdateCnt() {
		return updateCnt;
	}

	public WeightVector getWeight() {
		return weight;
	}

	private String identifyLocalServiceID(String serviceID) throws CloneNotSupportedException {
		List<EventDeclaration> complexEds = new ArrayList<EventDeclaration>();
		for (EventDeclaration ed : this.currentCompositionPlan.getEp().getEds()) {
			if (ed.getEp() != null)
				complexEds.add(ed);
			else if (serviceID.equals(ed.getServiceId()))
				return serviceID;
		}
		for (EventDeclaration ed : complexEds)
			for (EventDeclaration ed2 : ed.getEp().clone().getCompletePattern().getEds())
				if (ed2.getServiceId().equals(serviceID))
					return ed.getServiceId();
		return null;
	}

	@OnClose
	public void onClose(Session session, CloseReason closeReason) {
		logger.info(String.format("Session %s close because of %s", session.getId(), closeReason));
		latch.countDown();
	}

	@OnMessage
	public synchronized void onMessage(String message, Session session) throws Exception {
		if (message.contains("stopped")) {
			this.liveQosStreams.remove(message.split(" ")[1]);
		} else if (message.contains("sensorId")) {
			logger.warn("unexpected msg: " + message);
		} else {
			// logger.info("Received ...." + message);
			if (!adaptationLock) {
				QosUpdate qu = new Gson().fromJson(message, QosUpdate.class);
				this.setUpdateCnt(this.getUpdateCnt() + 1);
				String sid = qu.getCorrespondingServiceId();
				boolean isNew = false;
				QosVector newQos = qu.getQos();
				if (this.latestQosMap.containsKey(sid)) {
					if (!latestQosMap.get(sid).equals(newQos))
						isNew = true;
				} else
					isNew = true;

				QosVector currentQos = this.currentCompositionPlan.getEp().aggregateQos();
				logger.debug("received qos: " + qu.getQos().getAccuracy() + "for:"
						+ qu.getCorrespondingServiceId().split("#")[1] + " @" + qu.getObTimestamp());
				if (isNew) {
					long adaptationBegin = System.currentTimeMillis();
					this.latestQosMap.put(sid, newQos);
					for (EventDeclaration ed : this.currentCompositionPlan.getEp().clone().getCompletePattern()
							.getEds()) {
						if (ed.getServiceId().equals(sid)) {
							logger.info("updating qos: " + newQos + " for: " + sid.split("#")[1] + " @"
									+ qu.getObTimestamp());
							ed.updateExternalQos(newQos);
							break;
						}
					}
					currentQos = this.currentCompositionPlan.getEp().aggregateQos();
					logger.info("Current Total qos: " + currentQos);

					if (!currentQos.satisfyConstraint(constraint)) {
						logger.warn("Constraint Violated, const: " + constraint);
						this.adaptationLock = true;
						boolean success = false;

						EventDeclaration newPlan = this.adaptation(sid);
						if (this.mode != AdaptationMode.na)
							if (newPlan != null) {// adaptation success
								success = true;
								logger.info("Adaptation result: " + newPlan.getEp().toSimpleString());
								List<String> streamsToRemove = new ArrayList<String>();
								List<String> streamsToAdd = new ArrayList<String>();
								this.findDifferences(newPlan, streamsToRemove, streamsToAdd);
								Date restartDate = this.sub.unSubscribeToQosUpdate(session, streamsToRemove, freq);
								this.sub.subscribeToQosUpdate(newPlan, session, streamsToAdd, freq, restartDate);
								this.updateStreamReasoningQuery(newPlan, streamsToRemove, streamsToAdd);
								logger.info("current plan: " + this.currentCompositionPlan.getEp().toSimpleString());

							}
						this.adaptationLock = false;
						long adaptationComplete = System.currentTimeMillis();
						AdaptationResult ar = new AdaptationResult("AR-" + UUID.randomUUID(), success,
								adaptationComplete - adaptationBegin, qu.getObTimestamp());
						this.adaptations.add(ar);
						currentQos = this.currentCompositionPlan.getEp().aggregateQos();
					}
				}
				if (this.qosMap.get(qu.getObTimestamp()) == null)
					this.qosMap.put(qu.getObTimestamp(), new ArrayList<QosVector>());
				this.qosMap.get(qu.getObTimestamp()).add(currentQos);
				// logger.info("current plan: " + this.currentCompositionPlan.getEp().toSimpleString());
			}
		}
	}

	@OnOpen
	public void onOpen(Session session) {
		logger.info("Connected ... " + session.getId());
		this.liveQosStreams = this.sub.initializeQoSUpdates(currentCompositionPlan, session, freq, start, end);
	}

	public void setConstraint(QosVector constraint) {
		this.constraint = constraint;
	}

	public void setMode(AdaptationMode mode) {
		this.mode = mode;
	}

	public void setUpdateCnt(long updateCnt) {
		this.updateCnt = updateCnt;
	}

	public void setWeight(WeightVector weight) {
		this.weight = weight;
	}

	public void start(Double freq) throws Exception {
		latch = new CountDownLatch(1);
		try {
			logger.info("connecting...");
			client.connectToServer(this, new URI(uri));
			// new Thread(new QosMonitor()).start();
			latch.await(100, TimeUnit.SECONDS);
			AdpatationMonitor am = new AdpatationMonitor(currentCompositionPlan.getComposedFor().getID(),
					ACEISFactory.getACEISInstance());
			new Thread(am).start();
			// session.close();
		} catch (Exception e) {
			e.printStackTrace();
			throw new RuntimeException(e);

		}
	}

	private void updateStreamReasoningQuery(EventDeclaration newPlan, List<String> streamsToRemove,
			List<String> streamsToAdd) throws Exception {
		Date restartDate = SubscriptionManagerFactory.getSubscriptionManager().deregisterEventRequest(
				currentCompositionPlan, false);
		for (String s : streamsToRemove) {
			this.sub.stopCsparqlStream(s);
		}
		SubscriptionManagerFactory.getSubscriptionManager().registerEventRequest(engine, newPlan, null,
				this.sub.getQidSessionMap().get(this.currentCompositionPlan.getComposedFor().getID()), freq,
				restartDate, this.end, null, null);
		this.currentCompositionPlan = newPlan;
	}
}
