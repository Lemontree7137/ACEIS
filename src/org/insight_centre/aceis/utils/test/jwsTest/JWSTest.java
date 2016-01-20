package org.insight_centre.aceis.utils.test.jwsTest;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;

import org.insight_centre.aceis.io.rdf.RDFFileManager;
import org.insight_centre.aceis.io.streams.csparql.CSPARQLAarhusTrafficStream;
import org.insight_centre.aceis.io.streams.csparql.CSPARQLSensorStream;
import org.insight_centre.aceis.observations.SensorObservation;

import eu.larkc.csparql.engine.CsparqlEngine;
import eu.larkc.csparql.engine.CsparqlEngineImpl;
import eu.larkc.csparql.engine.CsparqlQueryResultProxy;

public class JWSTest {
	public static Map<SensorObservation, Long> latencyMap = new HashMap<SensorObservation, Long>();
	public static long inByteCnt = 0, outByteCnt = 0;
	public static CsparqlEngine csparqlEngine = null;
	public static Set<String> terminatedStreams = new HashSet<String>();
	public static Map<String, SensorObservation> obMap = new HashMap<String, SensorObservation>();
	public static Map<String, Long> inBytesMap = new HashMap<String, Long>();
	public static Map<Integer, Double> latencyOverTime = new HashMap<Integer, Double>();
	static Double rate = 0.5;
	private static String prefix = "";
	public static int mode = 0;

	public static double getAvgDelay() {
		int size = 0;
		long sum = 0;
		for (Entry<SensorObservation, Long> e : latencyMap.entrySet()) {
			size += 1;
			sum += (e.getValue() - e.getKey().getSysTimestamp().getTime());
		}
		System.out.println("Avg Delay: " + (sum + 0.0) / (size + 0.0) + ", Total: " + sum + ", Size: " + size
				+ ", Stopped: " + terminatedStreams.size());
		latencyMap.clear();
		return (sum + 0.0) / (size + 0.0);

	}

	public static void main(String[] args) throws Exception {

		csparqlEngine = new CsparqlEngineImpl();
		csparqlEngine.initialize(true);
		mode = 0;

		loadStreams(mode);
		registerQuery(mode, "2s step 1s");
		JWSMonitor monitor = new JWSMonitor();
		new Thread(monitor).start();
	}

	static boolean anyStreamStopped() {
		if (terminatedStreams.size() == 0)
			return false;
		else
			return true;
	}

	private static void loadStreams(int mode) throws ParseException {
		if (mode == 1) {
			rate = rate / 6;
			startStream(rate, "mean179282");
			startStream(rate, "mean179336");
			startStream(rate, "mean179390");
			startStream(rate, "mean180627");
			startStream(rate, "mean180681");
			startStream(rate, "mean180735");
			startStream(rate, "mean185131");
			startStream(rate, "mean187456");
			startStream(rate, "mean197814");
			startStream(rate, "mean201908");
		} else if (mode == 0) {
			startStream(rate, "216");
			startStream(rate, "218");
			startStream(rate, "220");
			startStream(rate, "226");
			startStream(rate, "228");
			startStream(rate, "230");
			startStream(rate, "289");
			startStream(rate, "322");
			startStream(rate, "540");
			startStream(rate, "537");
		} else {
			// String prefix = "";
			if (mode == 2) { // 1h
				prefix = "1h/";
				rate = rate / 12;
			} else if (mode == 3) { // 1.5 h
				prefix = "1h5/";
				rate = rate / 18;
			} else if (mode == 4) { // 2 h
				prefix = "2h/";
				rate = rate / 24;
			} else if (mode == 5) { // 2.5 h
				prefix = "2h5/";
				rate = rate / 48;
			}
			startStream(rate, "mean179282");
			startStream(rate, "mean179336");
			startStream(rate, "mean179390");
			startStream(rate, "mean180627");
			startStream(rate, "mean180681");
			startStream(rate, "mean180735");
			startStream(rate, "mean185131");
			startStream(rate, "mean187456");
			startStream(rate, "mean197814");
			startStream(rate, "mean201908");
		}

	}

	private static void registerQuery(int mode, String window) throws IOException, ParseException {
		String queryPath = "";
		if (mode == 0)
			queryPath = "dataset/jws/q3.txt";
		else
			queryPath = "dataset/jws/q2.txt";
		String query = new String(Files.readAllBytes(Paths.get(queryPath)));
		// System.out.println("Query: " + query);
		String originalWindow = query.substring(query.indexOf('[') + 1, query.indexOf(']'));
		System.out.println("window: " + originalWindow);
		query = query.replaceAll(originalWindow, "RANGE " + window);
		System.out.println("Query: " + query);
		CsparqlQueryResultProxy cqrp = csparqlEngine.registerQuery(query);
		JWSCsparqlResultFormatter cro = new JWSCsparqlResultFormatter();
		// RDFStreamFormatter cro = new RDFStreamFormatter(streamURI);
		cqrp.addObserver(cro);

	}

	static void startStream(Double sensorFreq, String file) throws ParseException {
		SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd");
		Date start = sdf.parse("2014-08-01");
		Date end = sdf.parse("2014-08-08");
		CSPARQLSensorStream ds = null;
		try {
			ds = new CSPARQLAarhusTrafficStream(RDFFileManager.defaultPrefix + file, "streams/jws/" + prefix + file
					+ ".stream", null, start, end);
			// ds.g
			((CSPARQLAarhusTrafficStream) ds).setForJWSTest(true);
			ds.setRate(sensorFreq);
			csparqlEngine.registerStream(ds);
			new Thread(ds).start();
		} catch (Exception e) {
			e.printStackTrace();
		}
	}
}
