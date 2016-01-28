package org.insight_centre.citypulse.server;

import java.io.IOException;

import javax.websocket.CloseReason;
import javax.websocket.OnClose;
import javax.websocket.OnMessage;
import javax.websocket.OnOpen;
import javax.websocket.Session;
import javax.websocket.server.ServerEndpoint;

import main.java.citypulse.commons.conflict_resolution.ConflictResolutionRequest;

import org.insight_centre.aceis.conflictresolution.SimpleSolver;
import org.insight_centre.aceis.conflictresolution.Solver;
import org.insight_centre.aceis.engine.ACEISEngine;
import org.insight_centre.aceis.engine.ACEISFactory;
import org.insight_centre.aceis.engine.ACEISScheduler;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.gson.Gson;

@ServerEndpoint(value = "/ConflictResolution")
public class ConflictResolutionServerEndpoint {
	private Logger logger = LoggerFactory.getLogger(getClass());
	private static ACEISEngine engine = ACEISScheduler.getAllACEISIntances().values().iterator().next();
	private static Solver solver = new SimpleSolver(engine);
	private Gson gson = new Gson();

	@OnClose
	public void onClose(Session session, CloseReason closeReason) throws IOException {
		logger.info(String.format("Session %s closed because of %s", session.getId(), closeReason));
		try {
			;
		} catch (Exception e) {
			session.getBasicRemote().sendText("FAULT: " + e.getMessage());
			logger.info("Sending error msg: " + e.getMessage());
		}
	}

	@OnMessage
	public String onMessage(String message, Session session) throws Exception {
		return solver.solve(gson.fromJson(message, ConflictResolutionRequest.class).getStreamIds());
	}

	@OnOpen
	public void onOpen(Session session) {
		logger.info("Subscriber connected ... " + session.getId());
		// sub.setTestSession(session);
	}
}
