package org.insight_centre.aceis.subscriptions;

import org.deri.cqels.data.Mapping;
import org.deri.cqels.engine.ContinuousListener;

public class TechnicalAdaptationListener implements ContinuousListener {
	private TechnicalAdaptationManager tam;
	private String serviceID;

	public TechnicalAdaptationListener(TechnicalAdaptationManager tam, String id) {
		this.tam = tam;
		this.serviceID = id;
	}

	@Override
	public void update(Mapping mapping) {
		// tam.adaptation(serviceID);

	}

}
