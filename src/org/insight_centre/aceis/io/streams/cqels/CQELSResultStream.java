package org.insight_centre.aceis.io.streams.cqels;

import java.io.IOException;
import java.io.PipedReader;
import java.io.PipedWriter;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.UUID;

import javax.websocket.Session;

import org.deri.cqels.data.Mapping;
import org.deri.cqels.engine.ContinuousListener;
import org.deri.cqels.engine.ContinuousSelect;
import org.deri.cqels.engine.ExecContext;
import org.deri.cqels.engine.RDFStream;
import org.insight_centre.aceis.engine.ACEISEngine;
import org.insight_centre.aceis.io.rdf.RDFFileManager;
import org.insight_centre.aceis.querytransformation.TransformationResult;
import org.insight_centre.citypulse.commons.data.json.JsonQuery;
import org.insight_centre.citypulse.commons.data.json.JsonQueryResult;
import org.insight_centre.citypulse.commons.data.json.JsonQueryResults;
import org.insight_centre.citypulse.commons.data.json.Location;
import org.insight_centre.citypulse.commons.data.json.Segment;
import org.insight_centre.citypulse.server.SubscriberServerEndpoint;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.gson.Gson;
import com.hp.hpl.jena.rdf.model.Model;
import com.hp.hpl.jena.rdf.model.ModelFactory;
import com.hp.hpl.jena.rdf.model.Property;
import com.hp.hpl.jena.rdf.model.Resource;
import com.hp.hpl.jena.rdf.model.Statement;
import com.hp.hpl.jena.sparql.core.Var;
import com.hp.hpl.jena.vocabulary.RDF;
import com.siemens.citypulse.resources.GlobalVariables.Modifier;
import com.siemens.citypulse.resources.GlobalVariables.Operator;

public class CQELSResultStream extends RDFStream implements Runnable {
	private static final Logger logger = LoggerFactory.getLogger(CQELSResultStream.class);
	// private List<PropertyName> requestedProperties;
	private Map<String, String> constraintMap;
	private JsonQuery jq;
	// private EventDeclaration plan;
	String[] latestResult;
	private Map<String, Modifier> modifierMap;
	private Map<String, Operator> operatorMap;
	private PipedReader pr;
	private PipedWriter pw;
	// private Map<String, Integer> messageCnt;
	// private File logFile;
	// private FileWriter fw;
	// private CsvWriter cw;
	private long recordTime, byteCnt, messageCnt;
	private ACEISEngine engine = null;
	private ContinuousSelect selQuery;
	private List<String> serviceList, propertyList, requestedProperties;
	private Session session = null;
	int sleep = 100; // check updates 10 times per second
	boolean stop = false;

	// public Map<String, Integer> getMessageCnt() {
	// return messageCnt;
	// }
	//
	// public void setMessageCnt(Map<String, Integer> messageCnt) {
	// this.messageCnt = messageCnt;
	// }

	// public int getSleep() {
	// return sleep;
	// }
	//
	// public void setSleep(int sleep) {
	// this.sleep = sleep;
	// }

	// public ResultStream(ExecContext context, String uri) {
	// super(context, uri);
	// //
	// }

	// private void addMessageCnt() {
	// if (this.getMessageCnt().get(this.getURI()) == null)
	// this.getMessageCnt().put(this.getURI(), 0);
	// int currentCnt = this.getMessageCnt().get(this.getURI());
	// this.getMessageCnt().put(this.getURI(), currentCnt + 1);
	// }

	public CQELSResultStream(ACEISEngine engine, final ExecContext context, String uri, ContinuousSelect selQuery,
			TransformationResult tr, Session session) throws IOException {
		super(context, uri);
		this.session = session;
		this.engine = engine;
		pr = new PipedReader();
		pw = new PipedWriter(pr);
		String fileName = "";
		if (this.getURI().split("#").length > 1)
			fileName = this.getURI().split("#")[1];
		else
			fileName = this.getURI();
		// logFile = new File("resultlog/" + fileName + ".csv");
		// fw = new FileWriter(logFile);
		// // pw.connect(pr);
		// cw = new CsvWriter(fw, ',');
		// cw.write("time");
		// cw.write("message_cnt");
		// cw.write("byte_cnt");
		// cw.endRecord();
		// cw.flush();
		recordTime = System.currentTimeMillis();
		byteCnt = 0;
		messageCnt = 0;
		recordTime = System.currentTimeMillis();
		this.serviceList = tr.getServiceList();
		this.propertyList = tr.getPropertyList();
		this.selQuery = selQuery;
		this.selQuery.register(new ContinuousListener() {

			@Override
			public void update(Mapping mapping) {
				// String updateId = UUID.randomUUID().toString();
				// logger.info("**************** CQELS update ");
				String result = "";
				try {
					for (Iterator<Var> vars = mapping.vars(); vars.hasNext();)
						result += " " + context.engine().decode(mapping.get(vars.next()));
				} catch (Exception e) {
					logger.error("CQELS decoding error: " + e.getMessage());
				}
				// logger.info("**************** CQELS result arrived: " +
				// result);
				try {
					pw.write(result + "|E|");
					// System.out.println("Writing result: " + result);
					pw.flush();
				} catch (IOException e) {

					// e.printStackTrace();
				}
			}
		});
	}

	// For citypulse scenario 1 integration: a json query is received to
	// indicate output functions&constraints
	public CQELSResultStream(final ExecContext context, String streamURI, ContinuousSelect registerSelect,
			TransformationResult tr, Session testSession, JsonQuery jq) throws IOException {

		super(context, streamURI);
		// context.
		this.session = testSession;
		pr = new PipedReader();
		pw = new PipedWriter(pr);
		this.jq = jq;
		this.constraintMap = jq.getConstraintMap();
		this.modifierMap = jq.getModifierMap();
		this.operatorMap = jq.getOperatorMap();
		this.requestedProperties = jq.getProperties();
		// String fileName = "";
		// if (this.getURI().split("#").length > 1)
		// fileName = this.getURI().split("#")[1];
		// else
		// fileName = this.getURI();
		// logFile = new File("resultlog/" + fileName + ".csv");
		// fw = new FileWriter(logFile);
		// // pw.connect(pr);
		// cw = new CsvWriter(fw, ',');
		// cw.write("time");
		// cw.write("message_cnt");
		// cw.write("byte_cnt");
		// cw.endRecord();
		// cw.flush();
		recordTime = System.currentTimeMillis();
		byteCnt = 0;
		messageCnt = 0;
		recordTime = System.currentTimeMillis();
		this.serviceList = tr.getServiceList();
		this.propertyList = tr.getPropertyList();
		this.selQuery = registerSelect;
		// selQuery.
		this.selQuery.register(new ContinuousListener() {
			private String id = UUID.randomUUID() + "";

			@Override
			public void update(Mapping mapping) {
				// logger.info("**************** CQELS update ");
				String result = "";
				try {
					for (Iterator<Var> vars = mapping.vars(); vars.hasNext();)
						result += " " + context.engine().decode(mapping.get(vars.next()));
				} catch (Exception e) {
					logger.error("CQELS decoding error: " + e.getMessage());
				}
				// logger.info("**************** CQELS result arrived: " +
				// result);
				try {
					pw.write(result + "|E|");
					// System.out.println("Writing result: " + result);
					pw.flush();
				} catch (IOException e) {

					// e.printStackTrace();
				}
			}
		});
		// logger.info("srv list: " + this.serviceList);
		// logger.info("p list: " + this.propertyList);
		// logger.info("req list: " + this.requestedProperties);
	}

	private String getAggregatedValue(Modifier modifier, List<String> values) throws Exception {
		// String result = "";
		switch (modifier) {
		case MAX: {
			double max = 0.0;
			for (String s : values) {
				if (Double.parseDouble(s) >= max)
					max = Double.parseDouble(s);
			}
			return max + "";
		}
		case MIN: {
			double min = 1000.0;
			for (String s : values) {
				if (Double.parseDouble(s) <= min)
					min = Double.parseDouble(s);
			}
			return min + "";
		}
		case AVG: {
			double sum = 0.0;
			for (String s : values) {
				sum += Double.parseDouble(s);
			}
			return sum / values.size() + "";
		}
		case SUM: {
			double sum = 0.0;
			for (String s : values) {
				sum += Double.parseDouble(s);
			}
			return sum + "";
		}
		}
		throw new Exception("Unidentifiable modifier.");
	}

	private Segment getPathSegment() {
		Double startLat = this.jq.getMsg().getStartPoint().getLat();
		Double startLon = this.jq.getMsg().getStartPoint().getLng();
		Double endLat = this.jq.getMsg().getEndPoint().getLat();
		Double endLon = this.jq.getMsg().getEndPoint().getLng();
		Location start = new Location();
		start.setLat(startLat);
		start.setLon(startLon);
		Location end = new Location();
		end.setLat(endLat);
		end.setLon(endLon);
		Segment result = new Segment(start, end);
		return result;
	}

	private Segment getSegment(String foiStr) {
		Double startLat = Double.parseDouble(foiStr.split("-")[0].split(",")[0]);
		Double startLon = Double.parseDouble(foiStr.split("-")[0].split(",")[1]);
		Double endLat = Double.parseDouble(foiStr.split("-")[1].split(",")[0]);
		Double endLon = Double.parseDouble(foiStr.split("-")[1].split(",")[1]);
		Location start = new Location();
		start.setLat(startLat);
		start.setLon(startLon);
		Location end = new Location();
		end.setLat(endLat);
		end.setLon(endLon);
		Segment result = new Segment(start, end);
		return result;

	}

	private boolean isInThreshold(String valueStr, Operator op, String constStr) throws Exception {
		if (op == null)
			return true;
		double valueDouble = Double.parseDouble(valueStr);
		double constDouble = Double.parseDouble(constStr);
		switch (op) {
		case DIFFERENTTO: {
			if (valueDouble != constDouble)
				return true;
			else
				return false;
		}
		case EQUALTO: {
			if (valueDouble == constDouble)
				return true;
			else
				return false;
		}
		case GREATERTHAN: {
			if (valueDouble > constDouble)
				return true;
			else
				return false;

		}
		case LESSTHAN: {
			if (valueDouble < constDouble)
				return true;
			else
				return false;

		}
		case GREATERTHANOREQUALTO: {
			if (valueDouble >= constDouble)
				return true;
			else
				return false;

		}
		case LESSTHANOREQUALTO: {
			if (valueDouble <= constDouble)
				return true;
			else
				return false;

		}
		}
		throw new Exception("Unidentifiable operator.");
	}

	@Override
	public void run() {
		logger.info("Starting result stream: " + this.getURI());
		try {
			String str = "";
			while (!stop) {
				int in = pr.read();
				// System.out.println("Reading: " + in);
				if ((System.currentTimeMillis() - recordTime) > 5000) {
					recordTime = System.currentTimeMillis();
					// cw.write(new SimpleDateFormat("hh:mm:ss").format(new
					// Date()));
					// cw.write(messageCnt + "");
					// cw.write(byteCnt + "");
					// cw.endRecord();
					// cw.flush();
					// fw.write(new Date() + "," + messageCnt + "," + byteCnt +
					// "\n");
					// fw.flush();
				}
				if (!str.contains("|E|") && in != -1) {
					str += (char) in;
					// in = pr.read();
					// System.out.println("Reading: " + str);
				} else {
					// this.addMessageCnt();

					logger.debug(this.getURI() + " results captured #" + this.messageCnt + ": " + str);
					// if ((System.currentTimeMillis() - recordTime) >= 5000)
					this.streamResultAsTriples(str.substring(0, str.indexOf("|E|")).trim().split(" "));
					// fw.write(new Date() + "," + byteCnt + "\n");
					// fw.wr
					// String[] data = str.split(" ");
					// if (data.length > 2)
					// stream(n(data[0]), n(data[1]), n(data[2])); // For
					// streaming RDF
					str = "";
					// Thread.sleep(sleep);
				}// triples
			}
			// cw.flush();
			// cw.close();
		} catch (IOException ioe) {
			logger.error("IO Exception: " + ioe.getMessage());
			// ioe.printStackTrace();
			this.stop();
		} catch (Exception e) {
			logger.error("Exception: " + e.getMessage());
			// e.printStackTrace();
			this.stop();
		} finally {
			this.stop();
			// try {
			// cw.flush();
			// cw.close();
			// } catch (IOException e) {
			// logger.error("ResultLogWriter failed to flush.");
			// e.printStackTrace();
			// }
		}
	}

	private void sendResult(Model m, String[] resultArray) throws Exception {
		if (this.jq == null) {
			List<Statement> stmts = m.listStatements().toList();
			for (Statement st : stmts) {
				stream(st.getSubject().asNode(), st.getPredicate().asNode(), st.getObject().asNode());
				logger.debug(this.getURI() + " Streaming Result: " + st.toString());
				// messageByte += st.toString().getBytes().length;
			}
		} else {
			// logger.info("Modifiers: " + jq.getModifierMap());
			// logger.info("Constraints: " + jq.getConstraintMap());
			// logger.info("Operators: " + jq.getOperatorMap());
			List<JsonQueryResult> results = new ArrayList<JsonQueryResult>();
			JsonQueryResults jqrs = new JsonQueryResults(results);
			for (String requestedProperty : this.requestedProperties) {
				Modifier modifier = this.modifierMap.get(requestedProperty);
				String value = this.constraintMap.get(requestedProperty);
				Operator op = this.operatorMap.get(requestedProperty);
				String abbreviatedPropertyStr = requestedProperty.replaceAll(" ", "").replaceAll("\\.", "");
				logger.debug("Abbrv property: " + abbreviatedPropertyStr);
				if (modifier != null) {
					List<String> values = new ArrayList<String>();
					for (int i = 0; i < this.propertyList.size(); i++) {
						// logger.info("values of "
						if (propertyList.get(i).contains(abbreviatedPropertyStr)) {
							String resultStr = resultArray[i * 2 + 1];
							resultStr = resultStr.replaceAll("\"", "");
							resultStr = resultStr.substring(0, resultStr.indexOf("^"));
							values.add(resultStr);
						}
					}
					logger.debug("values of " + requestedProperty + ": " + values);
					String aggValue = this.getAggregatedValue(modifier, values);
					JsonQueryResult jqr = new JsonQueryResult(requestedProperty, aggValue, modifier,
							this.getPathSegment(), this.isInThreshold(aggValue, op, value));
					results.add(jqr);
				} else {
					for (int i = 0; i < this.propertyList.size(); i++) {
						// logger.info("values of "
						if (propertyList.get(i).contains(abbreviatedPropertyStr)) {
							String resultStr = resultArray[i * 2 + 1];
							resultStr = resultStr.replaceAll("\"", "");
							resultStr = resultStr.substring(0, resultStr.indexOf("^"));
							String foi = this.engine.getRepo().getEds().get(serviceList.get(i)).getFoi();
							JsonQueryResult jqr = new JsonQueryResult(requestedProperty, resultStr, null,
									this.getSegment(foi), this.isInThreshold(resultStr, op, value));
							results.add(jqr);
						}
					}
				}
			}
			String resultStr = new Gson().toJson(jqrs);
			this.session.getBasicRemote().sendText(resultStr);
			logger.info("Sending query results: " + resultStr);
			SubscriberServerEndpoint.startSimulation = true;
		}

	}

	@Override
	public void stop() {
		if (!stop) {
			logger.info("Stopping stream: " + this.getURI());
			stop = true;
		}
		// Thread.

	}

	private void streamResultAsTriples(String[] resultArray) throws IOException {
		boolean isNew = false;
		if (latestResult == null) {
			isNew = true;
			// latestResult = resultArray;
		}
		int arraySize = resultArray.length;
		if (arraySize % 2 != 0)
			logger.error(this.getURI() + ": Result size incorrect. " + Arrays.asList(resultArray));
		else {
			Model m = ModelFactory.createDefaultModel();
			for (int i = 0; i < resultArray.length; i = i + 2) {
				String obIdStr = resultArray[i];
				if (!isNew)
					if (!obIdStr.equals(latestResult[i])) {
						isNew = true;
						break;
					}
				String serviceIdStr = serviceList.get(i / 2).replaceAll("<", "").replaceAll(">", "");
				// String typeStr = resultArray[i + 1];
				String valueStr = resultArray[i + 1];
				Resource observation = m.createResource(RDFFileManager.defaultPrefix + obIdStr);
				Property hasValue = m.createProperty(RDFFileManager.saoPrefix + "hasValue");
				observation.addProperty(RDF.type, m.createResource(RDFFileManager.ssnPrefix + "Observation"));
				Resource serviceID = m.createResource(serviceIdStr);
				observation.addProperty(m.createProperty(RDFFileManager.ssnPrefix + "observedBy"), serviceID);
				// Resource value;
				if (valueStr.contains("^^")) {
					if (valueStr.contains("double"))
						observation.addLiteral(hasValue, m.createTypedLiteral(Double.parseDouble(valueStr.substring(1,
								valueStr.indexOf("^^") - 1))));
					else if (valueStr.contains("string"))
						observation.addLiteral(hasValue,
								m.createTypedLiteral(valueStr.substring(1, valueStr.indexOf("^^") - 1)));
				} else
					observation.addProperty(hasValue, m.createResource(valueStr));
			}
			if (isNew) {
				// byteCnt += resultArray.toString().getBytes().length;
				messageCnt += 1;
				latestResult = resultArray;

				long messageByte = 0;
				logger.debug("Result array: " + Arrays.asList(resultArray));

				byteCnt += messageByte;
				latestResult = resultArray;
				// String consoleDisplay = "";
				// List<String> resultList = Arrays.asList(resultArray);
				// for (int i = 0; i < resultList.size(); i += 2) {
				// consoleDisplay += serviceList.get(i / 2) + "\t" +
				// resultList.get(i) + "\t" + resultList.get(i + 1)
				// + "\n";
				// }
				// consoleDisplay =
				// consoleDisplay.replaceAll(RDFFileManager.defaultPrefix,
				// "ServiceRepo#");
				// System.out.println("ResultStream - " + this.getURI() +
				// " captured result #" + this.messageCnt +
				// ": \n"
				// + consoleDisplay);
				try {
					this.sendResult(m, resultArray);
				} catch (Exception e) {
					e.printStackTrace();
					this.session.getBasicRemote().sendText("FAULT: " + e.getMessage());
					this.stop();
					logger.error("Sending error msg: " + e.getMessage());
				}
				// if (this.session != null)
				// this.session.getBasicRemote().sendText(consoleDisplay);
			} else {
				// logger.info("Duplicated message dicarded.");
			}
		}

	}

}
