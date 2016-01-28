package org.insight_centre.aceis.subscriptions.streams;

import java.io.IOException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;

import org.insight_centre.aceis.engine.ACEISEngine;
import org.insight_centre.aceis.engine.ACEISScheduler;
import org.insight_centre.aceis.eventmodel.QosVector;
import org.insight_centre.aceis.io.rdf.RDFFileManager;
import org.insight_centre.aceis.observations.QosUpdate;
import org.insight_centre.aceis.subscriptions.QosUpdateListener;
import org.insight_centre.citypulse.server.QosServerEndpoint;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.csvreader.CsvReader;
import com.google.gson.Gson;

public class QosUpdateStream implements Runnable {
	private List<QosUpdateListener> listeners = new ArrayList<QosUpdateListener>();
	private Logger logger = LoggerFactory.getLogger(this.getClass());
	private String observedServiceId;
	private final String path = "streams/qos/";
	private final SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss");
	private int sleep = 1000;
	private boolean stop = false;
	CsvReader streamData;
	private String txtFile;
	private QosVector currentQos;
	private String id;
	private Date start, end;

	public QosUpdateStream(String txtFile, String sid, Date start, Date end) throws IOException {
		super();
		// logger.info("Initializing qos stream: " + txtFile + " for: " + sid + ", " + start + " " + end);
		this.txtFile = txtFile;
		this.id = txtFile.split("\\.")[0];
		// this.observedServiceId = sid;
		if (txtFile.split("\\.")[0].split("-").length > 2)
			this.observedServiceId = RDFFileManager.defaultPrefix + sid.split("#")[1].split("-")[0] + "-"
					+ txtFile.split("\\.")[0].split("-")[2];
		else
			this.observedServiceId = RDFFileManager.defaultPrefix + sid.split("#")[1].split("-")[0];

		streamData = new CsvReader(String.valueOf(path + txtFile));
		streamData.setTrimWhitespace(false);
		streamData.setDelimiter(',');
		streamData.readHeaders();
		this.start = start;
		this.end = end;
		// logger.info("Initialized qos stream: " + this.id + " for: " + this.observedServiceId + ", " +
		// start.toString()
		// + " " + end.toString());
	}

	public synchronized void addListener(QosUpdateListener qul) {
		this.listeners.add(qul);
	}

	public List<QosUpdateListener> getListeners() {
		return listeners;
	}

	public String getObservedServiceId() {
		return observedServiceId;
	}

	public synchronized void removeListenter(String sessionId) {
		QosUpdateListener qulToRemove = null;
		for (QosUpdateListener qul : this.listeners) {
			if (qul.getSession().getId().equals(sessionId)) {
				qulToRemove = qul;
				break;
			}
		}
		if (qulToRemove != null)
			this.listeners.remove(qulToRemove);
	}

	@Override
	public void run() {
		try {
			logger.debug("Starting QoS update stream: " + this.txtFile);
			while (streamData.readRecord() && !stop) {
				String idStr = streamData.get("id");
				// String sensorIdStr = streamData.get("sensorId");
				Date timestamp = sdf.parse(streamData.get("timestamp"));
				logger.debug("Reading data: " + streamData.toString());
				if (this.start != null && this.end != null) {
					if (timestamp.before(this.start) || timestamp.after(this.end)) {
						logger.debug(this.id + ": Disgarded observation observed at: " + timestamp);
						continue;
					}
				}
				Double accuracy = Double.parseDouble(streamData.get("accuracy"));
				Integer latency = Integer.parseInt(streamData.get("latency"));
				Double completeness = Double.parseDouble(streamData.get("completeness"));
				Integer security = Integer.parseInt(streamData.get("security"));
				Double traffic = Double.parseDouble(streamData.get("traffic"));
				Integer price = Integer.parseInt(streamData.get("price"));
				QosVector qos = new QosVector();
				if (accuracy > 0.0)
					qos.setAccuracy(accuracy);
				if (latency > 0)
					qos.setLatency(latency);
				if (completeness > 0.0)
					qos.setReliability(completeness);
				if (security > 0)
					qos.setSecurity(security);
				if (traffic > 0.0)
					qos.setTraffic(traffic);
				if (price > 0)
					qos.setPrice(price);
				boolean isNew = false;
				if (this.currentQos == null) {
					currentQos = qos;
					isNew = true;
				} else if (!this.currentQos.equals(qos))
					isNew = true;
				// logger.info("Is new update: " + isNew);
				if (isNew)
					try {
						for (ACEISEngine engine : ACEISScheduler.getAllACEISIntances().values())
							engine.getRepo().getEds().get(observedServiceId).updateExternalQos(qos);
						// logger.info("Update repo qos for " + observedServiceId + " : " + qos);
					} catch (Exception e) {
						logger.error("Error updating qos: " + this.observedServiceId);
						e.printStackTrace();
					}
				QosUpdate qu = new QosUpdate(timestamp, idStr, observedServiceId, qos);
				this.currentQos = qos;
				this.sendUpdate(qu);
				// } else {
				// logger.debug(this.observedServiceId + ": no updates.");
				// }
				if (sleep > 0) {
					try {
						Thread.sleep(sleep);
					} catch (InterruptedException e) {
						e.printStackTrace();
					}
				}
			}
		} catch (Exception e) {
			e.printStackTrace();
		} finally {
			this.stop();
		}

	}

	public synchronized void sendUpdate(QosUpdate qu) {
		for (QosUpdateListener qul : this.listeners) {
			try {
				logger.debug(this.observedServiceId + " Sending qos update: " + qu.toString());
				qul.getSession().getBasicRemote().sendText(new Gson().toJson(qu));
				// ACEISEngine.getSubscriptionManager().getStreamCurrentTimeMap().put(this.id, qu.getObTimestamp());
				QosServerEndpoint.getStreamCurrentTimeMap().put(this.observedServiceId, qu.getObTimestamp());
				QosServerEndpoint.updateCnt += 1;
			} catch (Exception e) {
				e.printStackTrace();
			}
		}
	}

	public void setListeners(List<QosUpdateListener> listeners) {
		this.listeners = listeners;
	}

	public void setObservedServiceId(String observedServiceId) {
		this.observedServiceId = observedServiceId;
	}

	public void setRate(Double freq) {
		sleep = (int) (sleep / freq);
		// logger.info(this.observedServiceId + " Streamming interval set to: " + sleep + " ms");
	}

	public void stop() {
		this.stop = true;
		for (QosUpdateListener qul : this.listeners) {
			try {
				logger.debug(this.observedServiceId + " Sending stop: " + this.getObservedServiceId());
				qul.getSession().getBasicRemote().sendText("stopped: " + this.getObservedServiceId());
			} catch (Exception e) {
				e.printStackTrace();
			}
		}
	}
}
