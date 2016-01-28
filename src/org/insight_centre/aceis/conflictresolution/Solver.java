package org.insight_centre.aceis.conflictresolution;

import java.util.List;

import org.insight_centre.aceis.engine.ACEISEngine;
import org.insight_centre.aceis.eventmodel.NodeRemovalException;

public abstract class Solver {
	private ACEISEngine engine;

	public Solver(ACEISEngine engine) {
		super();
		this.engine = engine;
	}

	public ACEISEngine getEngine() {
		return engine;
	}

	public void setEngine(ACEISEngine engine) {
		this.engine = engine;
	}

	public abstract String solve(List<String> streamIds) throws Exception;
}
