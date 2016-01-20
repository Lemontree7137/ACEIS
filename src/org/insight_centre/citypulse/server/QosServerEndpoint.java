package org.insight_centre.citypulse.server;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

import javax.websocket.CloseReason;
import javax.websocket.OnClose;
import javax.websocket.OnMessage;
import javax.websocket.OnOpen;
import javax.websocket.Session;
import javax.websocket.server.ServerEndpoint;

import org.insight_centre.aceis.eventmodel.EventDeclaration;
import org.insight_centre.aceis.io.QosSubscription;
import org.insight_centre.aceis.subscriptions.QosUpdateListener;
import org.insight_centre.aceis.subscriptions.streams.QosUpdateStream;
import org.insight_centre.aceis.utils.test.qos.StreamQualityAnalyzer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.gson.Gson;

@ServerEndpoint(value = "/qos")
public class QosServerEndpoint {
	private Logger logger = LoggerFactory.getLogger(this.getClass());
	public static Map<String, QosUpdateStream> startedUpdateStreamMap = new HashMap<String, QosUpdateStream>();
	private static Map<String, Date> streamCurrentTimeMap = new HashMap<String, Date>();
	public static int updateCnt = 0;

	// private List<QosUpdateListener> registeredListenters = new ArrayList<QosUpdateListener>();

	@OnOpen
	public void onOpen(Session session) {
		logger.info("Technical Adaptation Manager connected ... " + session.getId());
		// sub.setTestSession(session);
	}

	@OnClose
	public void onClose(Session session, CloseReason closeReason) throws IOException {
		logger.info(String.format("Session %s closed because of %s", session.getId(), closeReason));
		// this.deregisterListener(session);

	}

	private synchronized void deregisterListener(Session session) {
		for (Entry<String, QosUpdateStream> e : this.startedUpdateStreamMap.entrySet()) {
			// for (QosUpdateStream qus : e.getValue()) {
			e.getValue().removeListenter(session.getId());
			// }
		}
	}

	private synchronized void deregisterListener(Session session, String sid) {
		for (Entry<String, QosUpdateStream> e : this.startedUpdateStreamMap.entrySet()) {
			// for (QosUpdateStream qus : e.getValue()) {
			if (e.getKey().equals(sid)) {
				e.getValue().removeListenter(session.getId());
				logger.info("qos listener removed for: " + session.getId() + " from: " + sid);
				// }
			}
		}
	}

	@OnMessage
	public String onMessage(String message, Session session) throws Exception {
		logger.info("Qos subscription received: " + message);
		QosSubscription qs = new Gson().fromJson(message, QosSubscription.class);
		List<String> sensorIds = qs.getSensorId();
		Double freq = qs.getFreq();
		// sensorIds.remove(sensorIds.size() - 1);
		if (!qs.isUnsubscribe())
			for (String sid : ((ArrayList<String>) sensorIds)) {
				try {
					// String extId = sid.split("#")[1].split("-")[0];
					// String reportId = StreamQualityAnalyzer.idMap.get(extId);
					// // String fileName = "";
					// if (extId.split("-").length > 1) {
					// reportId = reportId + "-" + extId.split("-")[1];
					// }
					QosUpdateListener qul = new QosUpdateListener(session);
					logger.info("subscribing qos: " + sid);
					// this.registeredListenters.add(qul); // check concurrent issues
					if (qs.getEngine().getRepo().getEds().get(sid).getEp() == null)
						this.subscribeToQoSStream(sid, qul, freq, qs.getStart(), qs.getEnd());
					else
						for (EventDeclaration ed : qs.getEngine().getRepo().getEds().get(sid).getEp()
								.getCompletePattern().getEds())
							this.subscribeToQoSStream(ed.getServiceId(), qul, freq, qs.getStart(), qs.getEnd());
				} catch (Exception e) {
					logger.error("Cannot subscribe to qos stream.");
					e.printStackTrace();
					session.getBasicRemote().sendText("ERROR: " + e.getMessage());
				}
			}
		else {
			for (String sid : ((ArrayList<String>) sensorIds)) {
				try {// check concurrent issues
					if (qs.getEngine().getRepo().getEds().get(sid).getEp() == null)
						this.deregisterListener(session, sid);
					else
						for (EventDeclaration ed : qs.getEngine().getRepo().getEds().get(sid).getEp()
								.getCompletePattern().getEds())
							this.deregisterListener(session, ed.getServiceId());
				} catch (Exception e) {
					logger.error("Cannot un-subscribe to qos stream.");
					e.printStackTrace();
					session.getBasicRemote().sendText("ERROR: " + e.getMessage());
				}
			}
		}
		return message;
	}

	private void subscribeToQoSStream(String sid, QosUpdateListener qul, Double freq, Date start, Date end)
			throws IOException {
		List<QosUpdateStream> qosStreams = new ArrayList<QosUpdateStream>();
		if (this.startedUpdateStreamMap.containsKey(sid)) {
			logger.info("Adding listener to existing: " + sid);
			this.startedUpdateStreamMap.get(sid).addListener(qul);
		} else {
			File dir = new File("streams/qos");
			File[] directoryListing = dir.listFiles();
			if (directoryListing != null) {
				for (File child : directoryListing) {
					if (child.getName().contains(
							"qos-" + StreamQualityAnalyzer.idMap.get(sid.split("#")[1].split("-")[0]))) {
						// logger.info("File " + child.getName() + " found for: " + sid);
						QosUpdateStream qus = new QosUpdateStream(child.getName(), sid, start, end);
						qus.setRate(freq);
						this.startedUpdateStreamMap.put(qus.getObservedServiceId(), qus);
						qosStreams.add(qus);
						// qosStreams.
					}
				}
			}
			// this.startedUpdateStreamMap.put(sid, qosStreams);
			for (QosUpdateStream qosStream : qosStreams) {
				if (qosStream.getObservedServiceId().equals(sid)) {
					logger.info("Adding listener: " + qul.toString() + "for :" + qosStream.getObservedServiceId());
					qosStream.addListener(qul);
				}
				new Thread(qosStream).start();
			}
		}

	}

	public static Map<String, Date> getStreamCurrentTimeMap() {
		return streamCurrentTimeMap;
	}

	// public void setStreamCurrentTimeMap(Map<String, Date> streamCurrentTimeMap) {
	// this.streamCurrentTimeMap = streamCurrentTimeMap;
	// }
}
