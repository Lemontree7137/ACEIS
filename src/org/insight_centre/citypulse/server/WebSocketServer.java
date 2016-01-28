package org.insight_centre.citypulse.server;

import java.io.BufferedReader;
import java.io.InputStreamReader;

//import org.deri.cqels.websocket.server.PublisherServerEndpoint;
import org.glassfish.tyrus.server.Server;

//import org.deri.cqels.websocket.server.CqelsManager;
//import org.deri.cqels.websocket.server.PublisherServerEndpoint;
//import org.deri.cqels.websocket.server.SubscriberServerEndpoint;
//import org.glassfish.tyrus.server.Server;

public class WebSocketServer {

	public WebSocketServer() {

	}

	public static void main(String[] args) {
		runServer();
	}

	public static void runServer() {
		// server information
		String hostIP = "127.0.0.1";// args[0];
		int port = 8080;// Integer.parseInt(args[1]);

		// initialize cqels engine

		Server server1 = new Server(hostIP, port, "/websockets", null, SubscriberServerEndpoint.class);
		// Server server2 = new Server(hostIP, port, "/websockets", null, QosServerEndpoint.class);
		try {
			server1.start();
			// server2.start();
			BufferedReader reader = new BufferedReader(new InputStreamReader(System.in));
			System.out.print("Please press a key to stop the server.");
			reader.readLine();
		} catch (Exception e) {
			throw new RuntimeException(e);
		} finally {
			server1.stop();
			// server2.stop();
		}
	}
}
