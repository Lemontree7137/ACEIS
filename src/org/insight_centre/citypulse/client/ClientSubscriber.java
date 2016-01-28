package org.insight_centre.citypulse.client;

import java.net.URI;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.logging.Logger;

import javax.websocket.ClientEndpoint;
import javax.websocket.CloseReason;
import javax.websocket.DeploymentException;
import javax.websocket.OnClose;
import javax.websocket.OnMessage;
import javax.websocket.OnOpen;
import javax.websocket.Session;

//import org.deri.cqels.websocket.client.ClientSubscriber;
//import org.deri.cqels.websocket.client.ClientSubscriber;
import org.glassfish.tyrus.client.ClientManager;
import org.insight_centre.aceis.io.QosSubscription;
import org.insight_centre.aceis.io.rdf.RDFFileManager;

import com.google.gson.Gson;

@ClientEndpoint
public class ClientSubscriber {
	private static final String jsonMsgFile = "ContextualFilter/Dataset/json_message_example.txt";
	private static CountDownLatch latch;
	private static String query = null;

	private static URI uri = null;

	public static void main(String[] args) throws Exception {
		// FileManager filemanager = FileManager.get();

		// query = filemanager.readWholeFileAsUTF8(args[0]);
		uri = new URI("ws://localhost:8080/websockets/subscribe-results");

		latch = new CountDownLatch(1);

		ClientManager client = ClientManager.createClient();
		try {
			Session session = client.connectToServer(ClientSubscriber.class, uri);
			latch.await(100, TimeUnit.DAYS);
			// client.g
		} catch (DeploymentException e) {
			throw new RuntimeException(e);
		}
	}

	private Logger logger = Logger.getLogger(this.getClass().getName());

	@OnClose
	public void onClose(Session session, CloseReason closeReason) {
		logger.info(String.format("Session %s close because of %s", session.getId(), closeReason));
		latch.countDown();
	}

	@OnMessage
	public void onMessage(String message, Session session) {
		logger.info("Received ...." + message);
	}

	@OnOpen
	public void onOpen(Session session) {
		logger.info("Connected ... " + session.getId());
		try {
			List<String> sids = new ArrayList<String>();
			sids.add(RDFFileManager.defaultPrefix + "235");
			QosSubscription qs = new QosSubscription(sids, 0.1, null, null);
			session.getBasicRemote().sendText(new Gson().toJson(qs));
			// System.out.println("Json Message loaded: " + jsonMsgStr);
		} catch (Exception e) {
			e.printStackTrace();
		}
		// try {
		// session.getBasicRemote().sendText(this.toString());
		// } catch (IOException e) {
		// throw new RuntimeException(e);
		// }
	}
}
