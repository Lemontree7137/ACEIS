package org.insight_centre.aceis.engine;

import com.hp.hpl.jena.rdf.model.Model;
//import citypulse.commons.event_request.DataFederationRequest;
//import citypulse.commons.event_request.DataFederationResult;
import com.hp.hpl.jena.query.Dataset;

public class DataFetchingEngine {
	private Dataset dataset;

	public DataFetchingEngine() {
		super();
		// this.dataset = RDFFileManager.dataset;
	}

	public Dataset getDataset() {
		return dataset;
	}

	public void setDataset(Dataset dataset) {
		this.dataset = dataset;
	}

	private Model loadSnapShot() {
		// SubscriptionManager sm=new SubscriptionManager();
		Model m = dataset.getDefaultModel();
		// m.add(SubscriptionManager.getSnapShot());
		return m;
	}

	// public DataFederationResult getDataFetchingResults(DataFederationRequest dataRequest) {
	// return null;
	//
	// }

}
