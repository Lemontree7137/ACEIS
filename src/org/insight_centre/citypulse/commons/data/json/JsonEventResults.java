package org.insight_centre.citypulse.commons.data.json;

import java.util.ArrayList;
import java.util.List;

public class JsonEventResults {
	private List<ContextualEvent> contextualEvents;

	public JsonEventResults() {
		setContextualEvents(new ArrayList<ContextualEvent>());
	}

	public JsonEventResults(List<ContextualEvent> contextualEvents) {
		super();
		this.setContextualEvents(contextualEvents);
	}

	public List<ContextualEvent> getContextualEvents() {
		return contextualEvents;
	}

	public void setContextualEvents(List<ContextualEvent> contextualEvents) {
		this.contextualEvents = contextualEvents;
	}

	void addEvent(ContextualEvent ce) {
		this.contextualEvents.add(ce);
	}
}
