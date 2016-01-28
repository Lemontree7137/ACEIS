package org.insight_centre.aceis.engine;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.concurrent.ConcurrentHashMap;

import org.insight_centre.aceis.engine.ACEISEngine.RspEngine;
import org.insight_centre.citypulse.main.MultipleInstanceMain;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

//import com.fasterxml.jackson.databind.util.LRUMap;
import com.google.common.cache.CacheBuilder;
import com.google.common.cache.CacheLoader;
import com.google.common.cache.LoadingCache;

public class ACEISScheduler {
	public class LruCache<String, Long> extends LinkedHashMap<String, Long> {
		private final int maxEntries;

		public LruCache(final int maxEntries) {
			super(maxEntries + 1, 1.0f, true);
			this.maxEntries = maxEntries;
		}

		protected boolean removeEldestEntry(final Map.Entry<String, Long> eldest) {
			return super.size() > maxEntries;
		}
	}

	// private
	public enum SchedulerMode {
		balancedLatency, balancedQueries, fixed, rotation, elastic;
	}

	private static int maxEngineCnt;
	private static int maxQueryCnt = 20;
	private static final Logger logger = LoggerFactory.getLogger(ACEISScheduler.class);
	private static List<ACEISEngine> cqelsList = new ArrayList<ACEISEngine>();
	private static List<ACEISEngine> csparqlList = new ArrayList<ACEISEngine>();
	private static HashMap<String, ACEISEngine> engineMap = new HashMap<String, ACEISEngine>();

	// private static LinkedHashMap<String, Long> obMap = new LinkedHashMap<String, Long>(16, 1.0f, true) {
	// protected boolean removeEldestEntry(final Map.Entry<String, Long> eldest) {
	// return super.size() > 1500;
	// }
	// };

	private static ConcurrentHashMap<String, List<String>> engineQueryMap = new ConcurrentHashMap<String, List<String>>();
	// private static Integer currentCqelsIndex = 0, currentCsparqlIndex = 0;
	private static int index = 0;

	static LoadingCache<String, Long> obMap = CacheBuilder.newBuilder().maximumSize(150000)
			.build(new CacheLoader<String, Long>() {
				public Long load(String key) {
					return null;
				}
			});

	public static SchedulerMode smode = SchedulerMode.fixed;
	private static String dataset;

	public static void addEngine(ACEISEngine engine) {
		engineMap.put(engine.getId(), engine);
		if (engine.getEngineType().equals(RspEngine.CSPARQL))
			csparqlList.add(engine);
		else
			cqelsList.add(engine);
		engineQueryMap.put(engine.getId(), new ArrayList<String>());
	}

	public static Map<String, ACEISEngine> getAllACEISIntances() {
		return engineMap;
	}

	private static long getAvgEngineLatency(ConcurrentHashMap<String, List<Long>> latencyMap, ACEISEngine engine) {
		List<String> qids = engineQueryMap.get(engine.getId());
		if (qids == null)
			return 0;
		else {
			ArrayList<Long> latestLatencies = new ArrayList<Long>();
			for (String qid : qids) {
				List<Long> latencies = latencyMap.get(qid);
				if (latencies != null && latencies.size() > 0) {
					latestLatencies.add(latencies.get(latencies.size() - 1));
				}
			}
			if (latestLatencies.size() > 0) {
				long latSum = 0;
				for (Long l : latestLatencies)
					latSum += l;
				return latSum / latestLatencies.size();
			} else
				return 0;
		}
	}

	private static ACEISEngine getEngineByQuery(String qid) {
		for (String eid : engineMap.keySet()) {
			if (!engineQueryMap.containsKey(eid))
				return null;
			else {
				List<String> qids = engineQueryMap.get(eid);
				if (qids.contains(qids))
					return engineMap.get(eid);
			}
		}
		return null;
	}

	public static ACEISEngine getBestEngineInstance(RspEngine engineType) throws Exception {
		List<ACEISEngine> engines = new ArrayList<ACEISEngine>();
		ACEISEngine result = null;
		// Integer index;
		if (engineType.equals(RspEngine.CQELS)) {
			engines = cqelsList;
			// index = currentCqelsIndex;
		} else {
			engines = csparqlList;
			// index = currentCsparqlIndex;
		}
		if (smode == SchedulerMode.rotation) {

			if (index >= engines.size())
				index = 0;
			result = engines.get(index);
			index += 1;
		} else if (smode.equals(SchedulerMode.balancedLatency)) {
			ConcurrentHashMap<String, List<Long>> latencyMap = MultipleInstanceMain.getMonitor().getGlobalLatencyMap();
			long minLat = 1000000;
			List<ACEISEngine> minLatEngines = new ArrayList<ACEISEngine>();
			Map<String, Long> engineLatMap = new HashMap<String, Long>();
			for (ACEISEngine engine : engines) {
				long avgLat = getAvgEngineLatency(latencyMap, engine);
				logger.info("engine: " + engine.getId() + " avg lat: " + avgLat);
				engineLatMap.put(engine.getId(), avgLat);
				if (avgLat <= minLat)
					minLat = avgLat;
			}
			for (Entry<String, Long> en : engineLatMap.entrySet()) {
				if (en.getValue() == minLat)
					minLatEngines.add(engineMap.get(en.getKey()));
			}
			int index = (int) (Math.random() * minLatEngines.size());
			result = minLatEngines.get(index);
		} else if (smode.equals(SchedulerMode.balancedQueries)) {

		} else if (smode.equals(SchedulerMode.fixed))
			result = engines.get(index);
		else if (smode.equals(SchedulerMode.elastic)) {
			// if (engines.size() >= maxEngineCnt)
			// smode = SchedulerMode.balancedLatency;
			// else {
			// ACEISEngine currentEngine = engines.get(engines.size() - 1);
			// else {
			if (engineQueryMap.get(engines.get(index).getId()).size() >= maxQueryCnt) {
				// } else {
				// RspEngine type = currentEngine.getEngineType();
				// if (type.equals(RspEngine.CQELS))
				// addEngine(ACEISFactory.getCQELSEngineInstance(ACEISScheduler.dataset));
				// else
				// addEngine(ACEISFactory.getCSPARQLEngineInstance(ACEISScheduler.dataset));
				// currentEngine = engines.get(engines.size() - 1);
				index += 1;
			}
			if (index >= maxEngineCnt) {
				smode = SchedulerMode.balancedLatency;
				index = index - 1;
			}
			result = engines.get(index);
		}

		// }
		return result;
	}

	public static ConcurrentHashMap<String, List<String>> getEngineQueryMap() {
		return engineQueryMap;

	}

	public static LoadingCache<String, Long> getObMap() {
		// obMap.get("");
		// obMap.
		return obMap;
	}

	public static void initACEISScheduler(int cqelsNum, int csparqlNum, String dataset) throws Exception {
		ACEISScheduler.dataset = dataset;
		// obMap = new LruCache<String, Long>(1000);
		// if (!smode.equals(SchedulerMode.elastic)) {
		for (int i = 0; i < cqelsNum; i++) {
			maxEngineCnt = cqelsNum;
			addEngine(ACEISFactory.getCQELSEngineInstance(dataset));
		}
		for (int i = 0; i < csparqlNum; i++) {
			addEngine(ACEISFactory.getCSPARQLEngineInstance(dataset));
			maxEngineCnt = csparqlNum;
		}
		// } else {
		// if (cqelsNum > 0) {
		// maxEngineCnt = cqelsNum;
		// addEngine(ACEISFactory.getCQELSEngineInstance(dataset));
		// }
		// if (csparqlNum > 0) {
		// maxEngineCnt = csparqlNum;
		// addEngine(ACEISFactory.getCSPARQLEngineInstance(dataset));
		// }
		// }
	}

	public static void main(String[] args) {

	}
}
