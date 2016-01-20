package org.insight_centre.aceis.utils.test;

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.concurrent.ConcurrentHashMap;

import org.insight_centre.aceis.engine.ACEISScheduler;
//import org.insight_centre.citybench.main.CityBench;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.csvreader.CsvWriter;

//import org.insight_centre.aceis.io.streams.cqels.CQELSResultListener;

public class PerformanceMonitor implements Runnable {
	private class PerformanceLog {
		private long cached;

		private double minlat, avgLat, maxLat, mem;

		private int resultCnt, qCnt, minuteCnt;

		public PerformanceLog(int minuteCnt, double minLat, double avglat, double maxLat, double mem, int resultCnt,
				int qCnt, long cached) {
			super();
			this.minlat = minLat;
			this.maxLat = maxLat;
			this.avgLat = avglat;
			this.mem = mem;
			this.resultCnt = resultCnt;
			this.qCnt = qCnt;
			this.cached = cached;
			this.minuteCnt = (minuteCnt);
		}

		public double getAvgLat() {
			return avgLat;
		}

		public long getCached() {
			return cached;
		}

		public double getMaxLat() {
			return maxLat;
		}

		public double getMem() {
			return mem;
		}

		public double getMinlat() {
			return minlat;
		}

		public int getMinuteCnt() {
			return minuteCnt;
		}

		public int getqCnt() {
			return qCnt;
		}

		public int getResultCnt() {
			return resultCnt;
		}

	}

	private static final Logger logger = LoggerFactory.getLogger(PerformanceMonitor.class);
	private CsvWriter cw;
	private int duplicates;
	// private Map<String, String> qMap;
	private long duration;
	private List<Long> globalLatencies = new ArrayList<Long>();
	private ConcurrentHashMap<String, List<Long>> globalLatencyMap = new ConcurrentHashMap<String, List<Long>>();;
	private ConcurrentHashMap<String, List<Long>> latencyMap = new ConcurrentHashMap<String, List<Long>>();
	private List<PerformanceLog> logs = new ArrayList<PerformanceLog>();
	private List<Double> memoryList = new ArrayList<Double>();
	private List<String> qList = new ArrayList<String>();
	private ConcurrentHashMap<String, Long> resultCntMap = new ConcurrentHashMap<String, Long>();
	private long resultInitTime = 0, lastCheckPoint = 0, globalInit = 0;
	private String resultName;
	private long start = 0;
	private boolean started = false;

	private boolean stop = false;

	public PerformanceMonitor(long duration, int duplicates, String resultName) {

		this.duration = duration;
		this.resultName = resultName;
		this.duplicates = duplicates;
		File outputFile = new File("resultlog/scalability" + File.separator + resultName + ".csv");
		if (outputFile.exists())
			logger.error("Result log file already exists.");
		try {
			cw = new CsvWriter(new FileWriter(outputFile, true), ',');
			cw.write("");
			cw.write("min-latency");
			cw.write("avg-latency");
			cw.write("max-latency");
			cw.write("memory");
			cw.write("results");
			cw.write("queries");
			cw.write("observations");
			cw.endRecord();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}

		// cw.flush();
		// cw.
		this.globalInit = System.currentTimeMillis();
	}

	public void addResults(String qid, Map<String, Long> results, int cnt) {
		if (this.resultInitTime == 0) {
			this.resultInitTime = System.currentTimeMillis();
			this.lastCheckPoint = System.currentTimeMillis();
		}
		// qid = qid.split("-")[0];
		if (!this.qList.contains(qid)) {
			this.qList.add(qid);
			this.latencyMap.put(qid, new ArrayList<Long>());
			this.globalLatencyMap.put(qid, new ArrayList<Long>());
			this.resultCntMap.put(qid, (long) 0);
		}
		for (Entry en : results.entrySet()) {
			String obid = en.getKey().toString();
			long delay = (long) en.getValue();
			this.latencyMap.get(qid).add(delay);
			this.globalLatencyMap.get(qid).add(delay);
			this.globalLatencies.add(delay);
		}
		this.resultCntMap.put(qid, this.resultCntMap.get(qid) + cnt);
	}

	public ConcurrentHashMap<String, List<Long>> getGlobalLatencyMap() {
		return this.globalLatencyMap;
	}

	public synchronized boolean isStarted() {
		return started;
	}

	public boolean isStop() {
		return stop;
	}

	// private void cleanup() {
	// if (CityBench.csparqlEngine != null) {
	// // CityBench.csparqlEngine.destroy();
	// for (Object css : CityBench.startedStreamObjects) {
	// ((CSPARQLSensorStream) css).stop();
	// }
	// } else {
	// // CityBench.cqelsContext.engine().
	// for (Object css : CityBench.startedStreamObjects) {
	// ((CQELSSensorStream) css).stop();
	// }
	// }
	// this.stop = true;
	// System.gc();
	//
	// }

	public void run() {
		int minuteCnt = 0;
		List<PerformanceLog> plList = new ArrayList<PerformanceLog>();
		while (!stop) {
			try {
				this.started = true;
				if (duration > 0)
					if (((System.currentTimeMillis() - this.globalInit) > 1.5 * duration)
							|| (duration != 0 && resultInitTime != 0 && (System.currentTimeMillis() - this.resultInitTime) > (60000 + duration))) {
						// writeLogs();
						logger.info("Stopping after " + (System.currentTimeMillis() - this.globalInit) + " ms.");
						logger.info("Experimment stopped.");
						System.exit(0);
					}

				if (this.lastCheckPoint != 0 && (System.currentTimeMillis() - this.lastCheckPoint) >= 60000) {
					minuteCnt += 1;
					this.lastCheckPoint = System.currentTimeMillis();
					this.summarsizeLogs(minuteCnt, plList);
					// logger.info("Results logged.");

					// empty memory and latency lists
					this.memoryList.clear();
					latencyMap.clear();
					qList.clear();
					plList.clear();
				}

				// Map<String, Double> currentLatency = new HashMap<String, Double>();
				// int activeQueries = 0;
				double latency = 0.0;
				int cnt = 0;
				for (String qid : this.qList) {
					for (long l : this.latencyMap.get(qid)) {
						latency += l;
						cnt += 1;
					}
				}
				latency = (latency + 0.0) / (cnt + 0.0);
				System.gc();
				Runtime rt = Runtime.getRuntime();
				double usedMB = (rt.totalMemory() - rt.freeMemory()) / 1024.0 / 1024.0;
				// double overhead = (obMapBytes + listerObIdListBytes + listenerResultListBytes) / 1024.0 / 1024.0;
				this.memoryList.add(usedMB);
				long resultCnt = 0;
				for (long resCntPart : this.resultCntMap.values())
					resultCnt += resCntPart;
				// logger.info("Current performance:  L - " + latency + ", Cnt: " + resultCnt + ", Mem - " + usedMB
				// + ", Active: " + this.qList.size() + ", Cached ObIds: " + ACEISScheduler.getObMap().size());// +
				// ", monitoring overhead - "
				PerformanceLog pl = new PerformanceLog(0, 0.0, latency, 0.0, usedMB, cnt, this.qList.size(),
						ACEISScheduler.getObMap().size());
				plList.add(pl);
				String engineQueryStr = "";
				for (Entry<String, List<String>> en : ACEISScheduler.getEngineQueryMap().entrySet()) {
					engineQueryStr += en.getValue().size() + ", ";
				}
				// logger.info("Engine status: " + engineQueryStr);
				Thread.sleep(5000);

			} catch (Exception e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		}
		System.exit(0);
	}

	public void setStop(boolean stop) {
		this.stop = stop;
	}

	private void summarsizeLogs(int minuteCnt, List<PerformanceLog> plList) {
		if (plList.size() > 0) {
			double avgLat = 0.0;
			double avgMem = 0.0;
			double minLat = 1000000.0;
			double maxLat = 0.0;
			int zeroCnt = 0;
			for (PerformanceLog pl : plList) {
				double lat = pl.getAvgLat();
				// logger.info("partial latency: " + lat);
				if (lat > 0)
					avgLat += lat;
				else
					zeroCnt += 1;
				if (lat <= minLat)
					minLat = lat;
				if (lat >= maxLat)
					maxLat = lat;
				avgMem += pl.getMem();
			}
			avgLat = avgLat / (plList.size() - zeroCnt + 0.0);
			avgMem = avgMem / (plList.size() + 0.0);
			int totalResultCnt = plList.get(plList.size() - 1).getResultCnt();
			int totalQueryCnt = plList.get(plList.size() - 1).getqCnt();
			long totalObCnt = plList.get(plList.size() - 1).getCached();
			// logger.info("summarzing: " + avgLat);
			PerformanceLog pl = new PerformanceLog(minuteCnt, minLat, avgLat, maxLat, avgMem, totalResultCnt,
					totalQueryCnt, totalObCnt);
			try {
				this.writeLog(pl);
			} catch (IOException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		} else
			logger.warn("No performance logs found.");
		// this.logs.add(pl);
	}

	private void writeLog(PerformanceLog pl) throws IOException {
		cw.write(pl.getMinuteCnt() + "");
		cw.write(pl.getMinlat() + "");
		cw.write(pl.getAvgLat() + "");
		cw.write(pl.getMaxLat() + "");
		cw.write(pl.getMem() + "");
		cw.write(pl.getResultCnt() + "");
		cw.write(pl.getqCnt() + "");
		cw.write(pl.getCached() + "");
		cw.endRecord();
		cw.flush();
		logger.info("Logging: minute - " + pl.getMinuteCnt() + ", lat - " + pl.getAvgLat() + ", mem - " + pl.getMem());
	}

	private void writeLogs() throws IOException {
		for (PerformanceLog pl : this.logs) {
			cw.write(pl.getMinuteCnt() + "");
			cw.write(pl.getMinlat() + "");
			cw.write(pl.getAvgLat() + "");
			cw.write(pl.getMaxLat() + "");
			cw.write(pl.getMem() + "");
			cw.write(pl.getResultCnt() + "");
			cw.write(pl.getqCnt() + "");
			cw.write(pl.getCached() + "");
			cw.endRecord();
		}
		cw.write("");
		cw.endRecord();
		cw.flush();
		List<Long> latencySnapShot = new ArrayList<Long>(globalLatencies);
		if (latencySnapShot != null) {
			// Collections.copy(globalLatencies, latencySnapShot);
			Collections.sort(latencySnapShot);
			int itemsToIgnore = (int) (latencySnapShot.size() * 0.05);
			for (int i = 0; i < itemsToIgnore; i++) {
				latencySnapShot.remove(latencySnapShot.size() - 1);
			}
			long min = latencySnapShot.get(0);
			long max = latencySnapShot.get(latencySnapShot.size() - 1);
			int interval = (int) ((max - min + 0.0) / 20.0);
			HashMap<Integer, Integer> latMap = new HashMap<Integer, Integer>();
			for (int i = 1; i < 21; i++) {
				latMap.put((int) (interval * i), 0);
			}
			// logger.info("lat map: " + latMap);
			for (long lat : latencySnapShot) {

				int index = 0;
				if (lat % interval != 0)
					index = (int) (lat / interval);
				else if (lat > interval)
					index = (int) (lat / interval) - 1;
				if (index > 19)
					index = 19;
				// logger.info("putting lat map: " + (interval * (index + 1)));
				latMap.put(interval * (index + 1), latMap.get(interval * (index + 1)) + 1);
			}
			for (int i = 1; i < 20; i++) {
				int scale = interval * i;
				int value = latMap.get(scale);
				cw.write(scale + "");
				cw.write(value + "");
				cw.endRecord();
			}
			cw.write(interval * 20 + "");
			cw.write(latMap.get(interval * 20) + itemsToIgnore + "");
			cw.endRecord();
			cw.flush();
		}
		cw.close();
	}

}
