package org.insight_centre.aceis.utils.test;

import org.insight_centre.aceis.engine.ACEISEngine;
import org.insight_centre.aceis.eventmodel.EventDeclaration;
import org.insight_centre.aceis.eventmodel.NodeRemovalException;
import org.insight_centre.aceis.io.streams.csparql.CSPARQLResultObserver;
import org.insight_centre.aceis.observations.SensorObservation;
import org.insight_centre.aceis.subscriptions.SubscriptionManagerFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class QosMonitor implements Runnable {

	private EventDeclaration plan;
	private static final Logger logger = LoggerFactory.getLogger(QosMonitor.class);
	private ACEISEngine engine;

	public QosMonitor(EventDeclaration plan) {
		super();
		this.plan = plan;
	}

	@Override
	public void run() {
		long start = System.currentTimeMillis();
		while (System.currentTimeMillis() - start <= (10 * 60000)) {
			try {
				Thread.sleep(5000);
			} catch (InterruptedException e) {
				e.printStackTrace();
				continue;
			}
			this.printResults();
		}
		this.printResults();
		try {
			logger.info("QoS: " + plan.getEp().aggregateQos());
		} catch (CloneNotSupportedException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (NodeRemovalException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		System.exit(0);
	}

	private Long getProduced(CSPARQLResultObserver cro) {

		return (long) cro.getProducedObservations().size();
	}

	// private Double getAccuracy(CSPARQLResultObserver cro) {
	// // double accSum = 0;
	// int cnt = 0, correctCnt = 0;
	// for (SensorObservation so : cro.getProducedObservations().values()) {
	//
	// AarhusTrafficObservation raw = (AarhusTrafficObservation) ACEISEngine.getSubscriptionManager().getObMap()
	// .get(so.getObId());
	// // logger.info("produced: " + so.getObId().split("#")[1] + " value: " + so.getValue() + "; raw: "
	// // + raw.getObId() + " value: " + raw.getVehicle_count());
	// if (so.getValue().equals(raw.getVehicle_count()))
	// correctCnt += 1;
	// cnt += 1;
	// }
	//
	// logger.info("correct: " + correctCnt + " cnt: " + cnt);
	// if (cro.getProducedObservations().size() > 0)
	// return (correctCnt + 0.0) / (cnt + 0.0);
	// return null;
	// }

	private Double getAccuracy(CSPARQLResultObserver cro) {

		return (cro.getCorrect() + 0.0) / (cro.getCorrect() + cro.getIncorrect() + 0.0);
		// return null;
	}

	private Long getConsumed(CSPARQLResultObserver cro) {
		return (long) SubscriptionManagerFactory.getSubscriptionManager().getConsumedMessageCnt().get(cro.getIRI());

	}

	private Double getAvgDelay(CSPARQLResultObserver cro) {
		double delaySum = 0;
		int cnt = 0;
		for (SensorObservation so : cro.getProducedObservations().values()) {
			long delay = so.getSysTimestamp().getTime()
					- SubscriptionManagerFactory.getSubscriptionManager().getObMap().get(so.getObId())
							.getSysTimestamp().getTime();
			delaySum += delay;
			cnt += 1;
		}
		if (cro.getProducedObservations().size() > 0)
			return delaySum / (cnt + 0.0);
		return null;
	}

	private void printResults() {
		// logger.info(ACEISEngine.getSubscriptionManager().getConsumedMessageCnt().toString());
		Runtime runtime = Runtime.getRuntime();
		runtime.gc();
		long memory = runtime.totalMemory() - runtime.freeMemory();
		logger.info("Used memory in bytes: " + memory + ", in megabytes: " + (memory + 0.0 / (1024 * 1024 * 1.0)));
		for (String s : SubscriptionManagerFactory.getSubscriptionManager().getCsparqlResultObservers().keySet()) {
			CSPARQLResultObserver cro = (CSPARQLResultObserver) SubscriptionManagerFactory.getSubscriptionManager()
					.getCsparqlResultObservers().get(s);
			// logger.info("cro: " + cro.getIRI());
			Double delay = this.getAvgDelay(cro);
			Long consumedMsgs = this.getConsumed(cro);
			Long producedMsgs = this.getProduced(cro);
			Double accuracy = this.getAccuracy(cro);
			logger.info(s.split("#")[1] + ", total avg. consumed: " + consumedMsgs + ", produced: " + producedMsgs
					+ ", delay: " + delay + ", accuracy: " + accuracy);
		}
	}
}
