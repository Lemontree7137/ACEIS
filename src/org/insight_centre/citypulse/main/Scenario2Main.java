package org.insight_centre.citypulse.main;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.InputStreamReader;
import java.net.URI;
import java.util.HashMap;
import java.util.Locale;
import java.util.Properties;

import org.glassfish.tyrus.server.Server;
import org.insight_centre.aceis.engine.ACEISEngine;
import org.insight_centre.aceis.engine.ACEISEngine.RspEngine;
import org.insight_centre.aceis.engine.ACEISFactory;
import org.insight_centre.citypulse.server.SubscriberServerEndpoint2;

public class Scenario2Main {
	private String hostIp;
	private int port;
	private String databaseURI;

	public Scenario2Main(Properties prop, HashMap<String, String> parameters) {
		Locale.setDefault(Locale.ENGLISH);
	}

	private static final URI localVirtuosoEndpoint = URI.create("http://localhost:8890/sparql");
	private static final URI UNISVirtuosoEndpoint = URI.create("http://iot.ee.surrey.ac.uk:8890/sparql");

	public static void main(String[] args) throws Exception {
		Properties prop = new Properties();
		// logger.info(Main.class.getClassLoader().);
		File in = new File("aceis.properties");
		FileInputStream fis = new FileInputStream(in);
		prop.load(fis);
		fis.close();
		// Thread.
		HashMap<String, String> parameters = new HashMap<String, String>();
		for (String s : args) {
			parameters.put(s.split("=")[0], s.split("=")[1]);
		}
		Scenario2Main cb = new Scenario2Main(prop, parameters);
		Scenario2Main.startServer();
		// Initialize the userPathGenerator
		// UserPathGenerator userPathGenerator = new UserPathGenerator();
		// // Generate the user's path
		// Location start = userPathGenerator.getRandomLocation();
		// Location end = userPathGenerator.getRandomLocation();
		// String transportType = "car"; // other option: "bike", "foot"
		// String constraint = "shortest";// other option: "fastest"
		// userPathGenerator.setParams(start, end, transportType, constraint);
		// Path path = userPathGenerator.generateUserPath();
		// System.out.println(path.toString());
		//
		// // Generate the events happen on the user's path
		// // EventGenerator eventGenerator = new EventGenerator();
		// // List<Event> events=
		// eventGenerator.generateEvents(userPathGenerator);
		// // generate the EventStream
		// // eventGenerator.generateEventStream();
		// List<ContextualEvent> events = new ArrayList();
		// UserStatus userStatus = new UserStatus("user_007", start,
		// Activity.DRIVING_CAR);
		// System.out.println("USER STATUS: " + userStatus.toString());
		//
		// // generate the UserLocationStream
		// userPathGenerator.generateUserLocationStream("user_007");
		//
		// // Filter
		// ContextualFilteringManager cfManager = new
		// ContextualFilteringManager(userStatus, path);
		// cfManager.addUpdateListener(new
		// ContextualFilteringListener(cfManager));
		// UserLocationStream userLocationStream = new
		// UserLocationStream(cfManager, System.getProperty("user.dir")
		// + "/ContextualFilter/Stream/UserLocationService.stream");
		// EventStream eventStream = new EventStream(cfManager,
		// System.getProperty("user.dir")
		// + "/ContextualFilter/Stream/EventService.stream");

		// server information

	}

	private static void startServer() throws Exception {
		String hostIP = "127.0.0.1";// args[0];
		int port = 8002;// Integer.parseInt(args[1]);
		HashMap<String, String> parameters = new HashMap<String, String>();

		// Logger logger = LoggerFactory.getLogger(CsparqlEngineImpl.class);
		// logger.isInfoEnabled()
		// initialize aceis engine
		ACEISEngine engine = ACEISFactory.createACEISInstance(RspEngine.CSPARQL, null);
		// engine.initialize(UNISVirtuosoEndpoint);
		// ACEISEngine.initialize(ACEISEngine.RspEngine.CSPARQL, "TrafficStaticData.n3");

		Server server = new Server(hostIP, port, "/websockets", null, SubscriberServerEndpoint2.class);
		// Server server2 = new Server(hostIP, port + 1, "/websockets", null, QosServerEndpoint.class);
		try {
			server.start();
			// server2.start();
			BufferedReader reader = new BufferedReader(new InputStreamReader(System.in));
			System.out.print("Please press a key to stop the server.");
			reader.readLine();
		} catch (Exception e) {
			throw new RuntimeException(e);
		} finally {
			// server.stop();
			// System.exit(0);
			// server2.stop();
		}

	}
}
