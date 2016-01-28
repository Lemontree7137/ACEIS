package org.insight_centre.citypulse.main;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileWriter;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.URI;
import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Locale;
import java.util.Properties;
import java.util.UUID;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import javax.websocket.ClientEndpointConfig;
import javax.websocket.CloseReason;
import javax.websocket.Endpoint;
import javax.websocket.EndpointConfig;
import javax.websocket.MessageHandler;
import javax.websocket.Session;
import javax.websocket.CloseReason.CloseCodes;

import org.glassfish.tyrus.client.ClientManager;
import org.glassfish.tyrus.server.Server;
import org.insight_centre.aceis.engine.ACEISEngine;
import org.insight_centre.aceis.engine.ACEISEngine.RspEngine;
import org.insight_centre.aceis.engine.ACEISScheduler;
import org.insight_centre.aceis.engine.ACEISScheduler.SchedulerMode;
import org.insight_centre.aceis.eventmodel.EventDeclaration;
import org.insight_centre.aceis.eventmodel.EventPattern;
import org.insight_centre.aceis.eventmodel.EventRequest;
import org.insight_centre.aceis.eventmodel.EventRequest.AggregateOperator;
import org.insight_centre.aceis.eventmodel.Selection;
import org.insight_centre.aceis.io.rdf.VirtuosoDataManager;
import org.insight_centre.aceis.utils.ServiceGenerator;
import org.insight_centre.aceis.utils.test.PerformanceMonitor;
import org.insight_centre.citypulse.commons.data.json.DataFederationRequest;
import org.insight_centre.citypulse.commons.data.json.DataFederationResult;
import org.insight_centre.citypulse.server.MultipleEngineServerEndpoint;
import org.insight_centre.citypulse.server.SubscriberServerEndpoint2;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import sun.misc.IOUtils;

import com.google.gson.Gson;

public class MultipleInstanceMain {
	private static final URI localVirtuosoEndpoint = URI.create("http://localhost:8890/sparql");
	private static final URI localhost = URI.create("127.0.0.1");
	private static final URI remoteHost = URI.create("131.227.92.55");
	private static final URI UNISVirtuosoEndpoint = URI.create("http://iot.ee.surrey.ac.uk:8890/sparql");
	private static final Logger logger = LoggerFactory.getLogger(MultipleInstanceMain.class);
	private static PerformanceMonitor pm;

	public static PerformanceMonitor getMonitor() {
		return pm;
	}

	public static void main(String[] args) throws Exception {
		Properties prop = new Properties();
		File in = new File("aceis.properties");
		FileInputStream fis = new FileInputStream(in);
		prop.load(fis);
		fis.close();
		HashMap<String, String> parameters = new HashMap<String, String>();
		for (String s : args) {
			parameters.put(s.split("=")[0], s.split("=")[1]);
		}
		MultipleInstanceMain mim = new MultipleInstanceMain(prop, parameters);

		mim.startServer();
		// mim.sendRequest();
	}

	private String hostIp, dataset, ontology, request, streams, query;
	private int port, cqelsCnt, csparqlCnt;
	private int qCnt, step;
	private long duration;
	private SchedulerMode smode = SchedulerMode.fixed;

	public MultipleInstanceMain(Properties prop, HashMap<String, String> parameters) {
		this.hostIp = prop.getProperty("hostIp");
		this.port = Integer.parseInt(prop.getProperty("port"));
		this.dataset = prop.getProperty("dataset");
		this.duration = Integer.parseInt(parameters.get("duration")) * 60000;
		this.request = prop.getProperty("request");
		this.ontology = prop.getProperty("ontology");
		this.streams = prop.getProperty("streams");
		this.cqelsCnt = Integer.parseInt(parameters.get("cqelsCnt"));
		this.csparqlCnt = Integer.parseInt(parameters.get("csparqlCnt"));
		String smodeStr = parameters.get("smode");
		this.query = parameters.get("query");
		if (smodeStr.equals("rotation"))
			this.smode = SchedulerMode.rotation;
		else if (smodeStr.equals("balancedLatency"))
			this.smode = SchedulerMode.balancedLatency;
		else if (smodeStr.equals("balancedQueries"))
			this.smode = SchedulerMode.balancedQueries;
		else if (smodeStr.equals("elastic"))
			this.smode = SchedulerMode.elastic;
		this.qCnt = Integer.parseInt(parameters.get("qCnt"));
		this.step = Integer.parseInt(parameters.get("step"));
		Locale.setDefault(Locale.ENGLISH);
		// VirtuosoDataManager.datasetDirectory = this.dataset;
		VirtuosoDataManager.ontologyDirectory = this.ontology;

		pm = new PerformanceMonitor(this.duration, 0, "cqelsCnt=" + this.cqelsCnt + ",csparqlCnt=" + this.csparqlCnt
				+ ",smode=" + this.smode + ",qCnt=" + this.qCnt + "-" + UUID.randomUUID());
		// VirtuosoDataManager.datasetDirectory
	}

	static void createRequests() throws Exception {
		ACEISEngine engine = ACEISScheduler.getBestEngineInstance(RspEngine.CSPARQL);

		List<EventPattern> queries = ServiceGenerator.createRandomAndQueries(engine, 4, 2500);
		// List<EventRequest> requests = new ArrayList<EventRequest>();
		List<String> results = new ArrayList<String>();
		for (EventPattern query : queries) {
			for (Selection sel : query.getSelections()) {
				sel.setPropertyName("pName-" + UUID.randomUUID());
			}
			query.setQuery(true);
			for (EventDeclaration ed : query.getEds()) {
				ed.setSrc(null);
				ed.setMsgBusGrounding(null);
				ed.setEid(null);
				ed.setRid(null);
				ed.setInternalQos(null);
				ed.setSensorId(null);
				ed.setFrequency(null);
				ed.setHttpGrounding(null);
				ed.setServiceId(null);
				ed.setPayloads(null);
				logger.info("ed: " + ed.toString());
				// ed.se
			}
			EventRequest er = new EventRequest(query, null, null);
			er.setAggOp(AggregateOperator.avg);
			er.setContinuous(true);
			// er.setContinuous(true);
			er.setEngineType(RspEngine.CQELS);
			String erStr = new Gson().toJson(er);
			results.add(erStr);
		}
		File queryFile = new File("queries3.txt");
		FileWriter fw = new FileWriter(queryFile);
		for (String s : results)
			fw.write(s + "\n");
		fw.flush();
		fw.close();
		// System.out.println(erStr);
	}

	private void startServer() throws Exception {
		ACEISScheduler.smode = this.smode;
		// ACEISScheduler.initACEISScheduler(cqelsCnt, csparqlCnt, dataset);
		// Server server = new Server(this.hostIp, this.port, "/", null, MultipleEngineServerEndpoint.class);
		// Server server2 = new Server(this.hostIp, 8002, "/", null, SubscriberServerEndpoint2.class);
		Session session = null;
		try {
			// server.start();
			// server2.start();
			session = sendOldRequest();
			// sendRequest(step);
			BufferedReader reader = new BufferedReader(new InputStreamReader(System.in));
			System.out.print("Please press a key to stop the server.");
			reader.readLine();
		} catch (Exception e) {
			e.printStackTrace();
			throw new RuntimeException(e);
		} finally {
			logger.info("Stopping Server.");
			session.close();
			// server.stop();
			System.exit(0);
		}

	}

	private Session sendOldRequest() {
		Session session = null;
		try {
			logger.info("sending request");
			final CountDownLatch messageLatch = new CountDownLatch(5);
			final ClientEndpointConfig cec = ClientEndpointConfig.Builder.create().build();
			// final List<EventRequest> requests = loadEventRequests();
			// StringBuilder sb = new StringBuilder();
			// Byte[] bytes = Files.readAllBytes("/dfr.txt");
			List<String> reqStrs = Files.readAllLines(new File("dfr.txt").toPath(), Charset.defaultCharset());
			String str = "";
			for (String s : reqStrs)
				str += s;
			DataFederationRequest dfr = new Gson().fromJson(str, DataFederationRequest.class);

			// String content = readFile("/dfr.txt", StandardCharsets.UTF_8);
			logger.info("String content: " + str);
			// DataFederationRequest dfr = new Gson().fromJson(content, DataFederationRequest.class);
			final ClientManager client = ClientManager.createClient();

			session = client.connectToServer(new Endpoint() {
				@Override
				public void onOpen(final Session session, final EndpointConfig config) {
					try {
						session.addMessageHandler(new MessageHandler.Whole<String>() {
							@Override
							public void onMessage(final String message) {
								System.out.println("Received message: " + message);
								try {
									DataFederationResult dfr = new Gson().fromJson(message, DataFederationResult.class);

									// System.out.println(dfr.getResult().containsKey(
									// VirtuosoDataManager.ctPrefix + "AverageSpeed"));
									// System.out.println(dfr.getResult().containsKey(
									// VirtuosoDataManager.ctPrefix + "AirPollutionIndex"));
									// System.out.println(dfr.getResult().keySet());
								} catch (Exception e) {
									e.printStackTrace();
								}
								messageLatch.countDown();
							}
						});

					} catch (final Exception e) {
						e.printStackTrace();
					}
				}
			}, cec, new URI("ws://" + remoteHost + ":" + 8002 + "/"));

			int cnt = 0;

			session.getBasicRemote().sendText(new Gson().toJson(dfr));
			// if (interval > 0)
			// Thread.sleep(interval);
			// }
			// Thread.sleep(25000);
			// session.getBasicRemote().sendText("quit");
			// session.getAsyncRemote().
			messageLatch.await(60, TimeUnit.MINUTES);
			// BufferedReader reader = new BufferedReader(new InputStreamReader(System.in));
			// System.out.print("Please press a key to stop the server.");
			// reader.readLine();
			session.close(new CloseReason(CloseCodes.NORMAL_CLOSURE, "Subscriber ended"));
			client.shutdown();
			return session;

		} catch (final Exception e) {
			e.printStackTrace();
		}
		return session;
	}

	private String readFile(String string, Charset utf8) {
		// TODO Auto-generated method stub
		return null;
	}

	private void sendRequest(final long interval) {
		try {
			logger.info("sending request");
			final CountDownLatch messageLatch = new CountDownLatch(1000);
			final ClientEndpointConfig cec = ClientEndpointConfig.Builder.create().build();
			final List<EventRequest> requests = loadEventRequests();
			final ClientManager client = ClientManager.createClient();

			Session session = client.connectToServer(new Endpoint() {
				@Override
				public void onOpen(final Session session, final EndpointConfig config) {
					try {
						session.addMessageHandler(new MessageHandler.Whole<String>() {
							@Override
							public void onMessage(final String message) {
								System.out.println("Received message: " + message);
								messageLatch.countDown();
							}
						});

					} catch (final Exception e) {
						e.printStackTrace();
					}
				}
			}, cec, new URI("ws://" + remoteHost + ":" + port + "/"));

			int cnt = 0;
			for (final EventRequest req : requests) {
				if (cnt < qCnt)
					cnt += 1;
				else
					break;
				session.getBasicRemote().sendText(new Gson().toJson(req));
				if (interval > 0)
					Thread.sleep(interval);
			}
			messageLatch.await(60, TimeUnit.MINUTES);

			client.shutdown();
		} catch (final Exception e) {
			e.printStackTrace();
		}

	}

	private List<EventRequest> loadEventRequests() throws FileNotFoundException {
		logger.info("loading queries.");
		List<EventRequest> results = new ArrayList<EventRequest>();
		try {
			List<String> reqStrs = Files.readAllLines(new File(query).toPath(), Charset.defaultCharset());
			String str = "";
			for (String s : reqStrs)
				str += s;
			EventRequest request = new Gson().fromJson(str, EventRequest.class);
			// for (String s : reqStrs) {
			// EventRequest request = new Gson().fromJson(s, EventRequest.class);
			if (this.cqelsCnt == 0)
				request.setEngineType(RspEngine.CSPARQL);
			else if (this.csparqlCnt == 0)
				request.setEngineType(RspEngine.CQELS);
			results.add(request);
			// // logger.info("query loaded");
			// }
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		// while(fr.r)
		return results;
	}
}
