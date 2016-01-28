package org.insight_centre.aceis.engine;

import java.net.URI;

import org.insight_centre.aceis.engine.ACEISEngine.RspEngine;
import org.insight_centre.aceis.io.EventRepository;

public class ACEISFactory {
	private static EventRepository baseER;

	public static ACEISEngine getCQELSEngineInstance(String dataset) throws Exception {
		return createACEISInstance(RspEngine.CQELS, dataset);
	}

	public static ACEISEngine getCSPARQLEngineInstance(String dataset) throws Exception {
		return createACEISInstance(RspEngine.CSPARQL, dataset);
	}

	public static ACEISEngine getACEISInstance() {
		return null;
	}

	public static ACEISEngine createACEISInstance(RspEngine engineType, String dataset) throws Exception {
		ACEISEngine engine = new ACEISEngine(engineType);
		if (dataset != null)
			if (!dataset.contains("http://"))
				engine.initialize(dataset);
			else
				engine.initialize(new URI(dataset));
		else
			throw new Exception("Dataset not specified.");
		return engine;
	}

	public static EventRepository getBaseER() {
		// TODO Auto-generated method stub
		return baseER;
	}

	public static void setBaseER(EventRepository clone) {
		baseER = clone;

	}
}
