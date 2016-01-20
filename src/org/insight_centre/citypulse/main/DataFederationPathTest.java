/**
 *
 */
package org.insight_centre.citypulse.main;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileWriter;
import java.io.IOException;
import java.net.URI;
import java.nio.charset.Charset;
import java.nio.file.Files;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import javax.websocket.ClientEndpointConfig;
import javax.websocket.Endpoint;
import javax.websocket.EndpointConfig;
import javax.websocket.MessageHandler;
import javax.websocket.Session;

import org.glassfish.tyrus.client.ClientManager;
import org.insight_centre.aceis.engine.ACEISEngine.RspEngine;
import org.insight_centre.aceis.eventmodel.EventPattern;
import org.insight_centre.aceis.eventmodel.EventRequest;
import org.insight_centre.aceis.eventmodel.EventRequest.AggregateOperator;
import org.insight_centre.aceis.utils.ServiceGenerator;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.gson.Gson;
//import citypulse.commons.data.Coordinate;
//import citypulse.commons.data.CoordinateParseException;
//import citypulse.commons.event_request.DataFederationRequest;
//import citypulse.commons.event_request.DataFederationRequest.DataFederationPropertyType;
import org.insight_centre.aceis.engine.ACEISEngine;
import org.insight_centre.aceis.engine.ACEISScheduler;

/**
 * @author Stefano Germano
 *
 */
public class DataFederationPathTest {
	private static final String surreyserver = "131.227.92.55";
	private static final String localhost = "127.0.0.1";
	/**
	 *
	 */
	private static CountDownLatch messageLatch;
	private static String parking_path = "10.20496 56.14851,10.20696 56.15051";
	private static String message_path = "10.1580864 56.147735,10.1579134 56.1478898,10.1571316 56.1486248,10.1560721 56.1483275,10.1559691 56.1482985,"
			+ "10.1558718 56.1482019,10.1553066 56.147856,10.1539538 56.1485603,10.1530389 56.1490447,10.1507527 56.1502595,"
			+ "10.1495063 56.149666,10.1488428 56.1493557,10.1488778 56.1493428,10.148773 56.1493045,10.1487928 56.1492891,"
			+ "10.1486534 56.149088,10.1487025 56.1490487,10.1486695 56.1490211,10.1485742 56.1489903,10.1477887 56.1494544,"
			+ "10.1469023 56.1499329,10.1467035 56.1500824,10.14651 56.1502408,10.1462984 56.1501796,10.1461946 56.1501633,"
			+ "10.1460163 56.1502989,10.1458366 56.1504637,10.14563 56.1506843,10.1455469 56.1508298,10.1454584 56.1509583";
	private static final Logger logger = LoggerFactory.getLogger(DataFederationPathTest.class);

	// + "10.144997 56.1515036,10.1444043 56.1521933,10.1440636 56.152648,10.1439 56.1528481,10.1437552 56.1530005,"
	// + "10.1436586 56.1531828,10.1435808 56.153259,10.1435191 56.1533068,10.1434145 56.1533376,10.1433046 56.1533391,"
	// +
	// "10.1417945 56.1532156,10.1415209 56.1532002,10.1411105 56.1531902,10.1407457 56.1531843,10.1377121 56.1529975,"
	// +
	// "10.1375218 56.1529626,10.1373366 56.1529467,10.1370845 56.1529238,10.1367508 56.1529031,10.1365467 56.1528946,"
	// +
	// "10.1363618 56.1528864,10.1357334 56.1528538,10.1353678 56.1528306,10.1351245 56.1528286,10.1348628 56.1528347,"
	// + "10.1345686 56.1528377,10.1340992 56.1528227,10.131358 56.1526973,10.1302533 56.152625,10.1298765 56.1525994,"
	// +
	// "10.1292326 56.1525532,10.1284693 56.1524971,10.1281849 56.1524522,10.1278872 56.1524298,10.1277343 56.1524194,"
	// + "10.1275385 56.1524209,10.1270021 56.1523925,10.1265273 56.1523985,10.1262135 56.1524,10.124918 56.1524224";

	// +
	// "10.1238493 56.1524815,10.1214741 56.1525987,10.1207888 56.1526325,10.1187275 56.1527391,10.1185558 56.1527256,"
	// + "10.118293 56.1527212,10.1180006 56.1527226,10.1178316 56.1527361,10.1177002 56.1527391,10.1168231 56.1526793,"
	// + "10.1162491 56.152645,10.1159728 56.1526405,10.1156107 56.1526435,10.115085 56.1526689,10.114511 56.1526958,"
	// + "10.1138941 56.1527406,10.1132638 56.1528093,10.1130641 56.1528357,10.1130315 56.1526304,10.112703 56.1526471,"
	// + "10.1125615 56.1526762,10.112407 56.1527286,10.1118373 56.1529252,10.1114757 56.1530299,10.1112497 56.1530834,"
	// +
	// "10.1110222 56.1531224,10.1105534 56.1531874,10.1101817 56.1532165,10.1098297 56.1532297,10.1098136 56.1531072,"
	// + "10.1083813 56.1531819,10.1083977 56.153309,10.1080487 56.1533283,10.1071689 56.1533672,10.1065306 56.1533791,"
	// +
	// "10.1054952 56.1533911,10.1049642 56.1533941,10.1044599 56.1533731,10.1042084 56.1533418,10.1040443 56.1533243,"
	// +
	// "10.1034211 56.1532287,10.1026965 56.1531168,10.1024405 56.1530973,10.1021809 56.1531017,10.1013514 56.1531743,"
	// +
	// "10.0999177 56.1533293,10.0990944 56.1534158,10.0989404 56.1534509,10.0988162 56.1534926,10.0986993 56.1535433,"
	// + "10.0985492 56.15368,10.0978599 56.1535467,10.0970298 56.1534597,10.0946089 56.1532886,10.0940557 56.1532346,"
	// + "10.0936227 56.1531825,10.0931178 56.1531756,10.0919419 56.1530978,10.0906976 56.1530153,10.0902475 56.152976,"
	// + "10.089983 56.1529518,10.0889423 56.1528651,10.0885098 56.1528465,10.0880438 56.1528394,10.0867931 56.1527642,"
	// +
	// "10.0850229 56.1526333,10.0781996 56.1521946,10.0774935 56.1521815,10.0762975 56.1522415,10.0761896 56.1522469,"
	// + "10.075466 56.152242,10.074726 56.1521448,10.0740552 56.1519876,10.0734983 56.1518977,10.0732517 56.1518711";

	/**
	 * @param args
	 */
	public static void main(final String[] args) {
		sendRequest();
		// try {
		// DataFederationPathTest.messageLatch = new CountDownLatch(1000);
		//
		// final ClientEndpointConfig cec = ClientEndpointConfig.Builder.create().build();
		//
		// final ClientManager client = ClientManager.createClient();
		// client.connectToServer(new Endpoint() {
		//
		// @Override
		// public void onOpen(final Session session, final EndpointConfig config) {
		// try {
		// session.addMessageHandler(new MessageHandler.Whole<String>() {
		// @Override
		// public void onMessage(final String message) {
		// System.out.println("Received message: " + message);
		// DataFederationPathTest.messageLatch.countDown();
		// }
		// });
		//
		// final List<Coordinate> path = new LinkedList<>();
		// final String[] split = DataFederationPathTest.message_path.split(",");
		// // final String[] split = DataFederationPathTest.parking_path.split(",");
		// for (final String c : split)
		// path.add(new Coordinate(c));
		// final List<DataFederationPropertyType> property = new LinkedList<>();
		// property.add(DataFederationPropertyType.average_speed);
		// // property.add(DataFederationPropertyType.air_quality);
		// // property.add(DataFederationPropertyType.parking_availability);
		// String testStr = FileUtils.readWholeFileAsUTF8("testJsonStr.txt");
		// // session.getBasicRemote().sendText(testStr);
		// session.getBasicRemote().sendText(
		// new Gson().toJson(new DataFederationRequest(RspEngine.CQELS, property, path, true,
		// null, null)));
		//
		// } catch (final Exception e) {
		// e.printStackTrace();
		// }
		// }
		// }, cec, new URI("ws://" + localhost + ":8002/websockets" + "/DataFederation/multiple"));
		// DataFederationPathTest.messageLatch.await(1000, TimeUnit.SECONDS);
		// client.shutdown();
		// } catch (final Exception e) {
		// e.printStackTrace();
		// }

	}

	private static void sendRequest() {
		try {
			final CountDownLatch messageLatch = new CountDownLatch(1000);
			final ClientEndpointConfig cec = ClientEndpointConfig.Builder.create().build();
			List<EventRequest> requests = loadEventRequests();
			final ClientManager client = ClientManager.createClient();
			for (final EventRequest req : requests) {
				client.connectToServer(new Endpoint() {
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

							session.getBasicRemote().sendText(new Gson().toJson(req));

						} catch (final Exception e) {
							e.printStackTrace();
						}
					}
				}, cec, new URI("ws://" + localhost + ":8002/websockets" + "/DataFederation/multiple"));
				messageLatch.await(1000, TimeUnit.SECONDS);
			}
			client.shutdown();
		} catch (final Exception e) {
			e.printStackTrace();
		}

	}

	private static List<EventRequest> loadEventRequests() throws FileNotFoundException {
		logger.info("loading queries.");
		List<EventRequest> results = new ArrayList<EventRequest>();
		try {
			List<String> reqStrs = Files.readAllLines(new File("queries.txt").toPath(), Charset.defaultCharset());
			for (String s : reqStrs) {
				EventRequest request = new Gson().fromJson(s, EventRequest.class);
				request.setEngineType(RspEngine.CSPARQL);
				results.add(request);
				logger.info("query loaded");
			}
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		// while(fr.r)
		return results;
	}

	static void createRequests() throws Exception {
		ACEISEngine engine = ACEISScheduler.getBestEngineInstance(RspEngine.CSPARQL);

		List<EventPattern> queries = ServiceGenerator.createRandomAndQueries(engine, 5, 3);
		List<EventRequest> requests = new ArrayList<EventRequest>();
		List<String> results = new ArrayList<String>();
		for (EventPattern query : queries) {
			EventRequest er = new EventRequest(query, null, null);
			er.setAggOp(AggregateOperator.avg);
			er.setContinuous(true);
			// er.setContinuous(true);
			er.setEngineType(RspEngine.CQELS);
			String erStr = new Gson().toJson(er);
			results.add(erStr);
		}
		File queryFile = new File("queries.txt");
		FileWriter fw = new FileWriter(queryFile);
		for (String s : results)
			fw.write(s + "\n");
		fw.flush();
		fw.close();
		// System.out.println(erStr);
	}
}
