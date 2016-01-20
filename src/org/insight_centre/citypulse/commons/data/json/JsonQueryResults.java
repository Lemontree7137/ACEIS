package org.insight_centre.citypulse.commons.data.json;

import java.util.ArrayList;
import java.util.List;

public class JsonQueryResults {
	private List<JsonQueryResult> results;

	// private List<ContextualEvent> contextualEvents;

	public JsonQueryResults() {
		results = new ArrayList<JsonQueryResult>();
		// contextualEvents = new ArrayList<ContextualEvent>();
	}

	public JsonQueryResults(List<JsonQueryResult> results) {
		this.results = results;
		// this.contextualEvents = contextualEvents;
	}

	public List<JsonQueryResult> getResults() {
		return results;
	}

	public void setResults(List<JsonQueryResult> results) {
		this.results = results;
	}

	void addResult(JsonQueryResult result) {
		this.results.add(result);
	}

	// void addEvent(ContextualEvent evt) {
	// this.contextualEvents.add(evt);
	// }
}
