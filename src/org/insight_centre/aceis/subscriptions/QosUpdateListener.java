package org.insight_centre.aceis.subscriptions;

import java.io.IOException;

import javax.websocket.Session;

import org.insight_centre.aceis.observations.QosUpdate;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.gson.Gson;

public class QosUpdateListener {
	private Session session;
	private Logger logger = LoggerFactory.getLogger(this.getClass());

	public Session getSession() {
		return session;
	}

	public QosUpdateListener(Session session) {
		super();
		this.session = session;
		// this.session.get
	}

	public void setSession(Session session) {
		this.session = session;
	}

	public void sendUpdate(QosUpdate qu) throws IOException {
		this.session.getBasicRemote().sendText(new Gson().toJson(qu));
	}
}
