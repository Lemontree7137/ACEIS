package org.insight_centre.aceis.io.streams.cqels;

import java.io.IOException;
import java.io.PipedReader;
import java.io.PipedWriter;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.concurrent.LinkedBlockingQueue;

import org.deri.cqels.data.Mapping;
import org.deri.cqels.engine.ContinuousListener;
import org.deri.cqels.engine.ContinuousSelect;
import org.deri.cqels.engine.ExecContext;
import org.deri.cqels.engine.RDFStream;
import org.insight_centre.aceis.engine.ACEISEngine;
import org.insight_centre.aceis.engine.ACEISScheduler;
import org.insight_centre.aceis.io.rdf.VirtuosoDataManager;
import org.insight_centre.aceis.querytransformation.TransformationResult;
import org.insight_centre.citypulse.main.MultipleInstanceMain;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.hp.hpl.jena.rdf.model.Model;
import com.hp.hpl.jena.rdf.model.ModelFactory;
import com.hp.hpl.jena.rdf.model.Property;
import com.hp.hpl.jena.rdf.model.Resource;
import com.hp.hpl.jena.rdf.model.Statement;
import com.hp.hpl.jena.sparql.core.Var;
import com.hp.hpl.jena.vocabulary.RDF;

public class SimpleCQELSResultStream extends RDFStream implements Runnable {
	public SimpleCQELSResultStream(ExecContext context, String uri) {
		super(context, uri);
		// TODO Auto-generated constructor stub
	}

	private static final Logger logger = LoggerFactory.getLogger(SimpleCQELSResultStream.class);
	// private List<PropertyName> requestedProperties;
	// private Map<String, String> constraintMap;
	// private JsonQuery jq;
	// private EventDeclaration plan;
	// String[] latestResult;
	// private Map<String, Modifier> modifierMap;
	// private Map<String, Operator> operatorMap;
	private PipedReader pr;
	private PipedWriter pw;
	// private Map<String, Integer> messageCnt;
	// private File logFile;
	// private FileWriter fw;
	// private CsvWriter cw;
	// private String uri;
	private long recordTime, byteCnt, messageCnt;
	private ACEISEngine engine = null;
	private ContinuousSelect selQuery;
	private List<String> serviceList, sensorList, propertyList, requestedProperties;
	private LinkedBlockingQueue<String> cache = new LinkedBlockingQueue<String>(1000);
	private LinkedBlockingQueue<String> capturedObIds = new LinkedBlockingQueue<String>(1000);
	// private Session session = null;
	int sleep = 100; // check updates 10 times per second
	boolean stop = false;

	public SimpleCQELSResultStream(ACEISEngine engine, final ExecContext context, String uri, TransformationResult tr)
			throws IOException {
		super(context, uri);
		// this.session = session;
		// this.uri = uri;
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
		// recordTime = System.currentTimeMillis();
		byteCnt = 0;
		messageCnt = 0;
		recordTime = System.currentTimeMillis();
		this.serviceList = tr.getServiceList();
		this.sensorList = tr.getSensorList();
		this.propertyList = tr.getPropertyList();
		this.selQuery = context.registerSelect(tr.getQueryStr());
		this.selQuery.register(new ContinuousListener() {

			@Override
			public void update(Mapping mapping) {

				// String updateId = UUID.randomUUID().toString();
				// logger.info("**************** CQELS update ");

				String result = "";
				try {

					for (Iterator<Var> vars = mapping.vars(); vars.hasNext();) {
						result += " " + context.engine().decode(mapping.get(vars.next()));
					}
					if (!cache.contains(result)) {
						// try {
						// pw.write(result + "|E|");
						// // System.out.println("Writing result: " + result);
						// pw.flush();
						// if (cache.remainingCapacity() <= 1)
						// cache.poll();
						// cache.put(result);
						// } catch (IOException | InterruptedException e) {
						// logger.error("CQELS pipeline error: " + e.getMessage());
						// // e.printStackTrace();
						// }

						// no streamming results below
						long receivedTime = System.currentTimeMillis();
						Map<String, Long> latencies = new HashMap<String, Long>();
						String str2 = result.trim();
						String[] vars = str2.split(" ");
						for (int i = 0; i < vars.length; i += 2) {
							if (!capturedObIds.contains(vars[i])) {
								if (capturedObIds.remainingCapacity() <= 1) {
									capturedObIds.poll();
								}
								capturedObIds.put(vars[i]);
								// logger.info("looking for: " + vars[i]);
								long t1 = System.currentTimeMillis();
								long initTime = ACEISScheduler.getObMap().get(vars[i].split("#")[1]);
								long t2 = System.currentTimeMillis();
								if (t2 - t1 >= 100) {
									logger.warn("Cache lookup time: " + (t2 - t1));
								}
								latencies.put(vars[i], (receivedTime - initTime));
							}
						}

						MultipleInstanceMain.getMonitor().addResults(getURI(), latencies, 1);
					} else {
						logger.debug("Duplicated.");
					}
					logger.debug("*** CQELS result: " + result);
				} catch (Exception e) {
					// e.printStackTrace();
					logger.error("CQELS decoding error: " + e.getMessage());
				}

			}
		});
	}

	@Override
	public void run() {
		logger.debug("Starting result stream: " + this.getURI());
		try {
			String str = "";
			// while (!stop) {
			// int in = pr.read();
			// // System.out.println("Reading: " + in);
			//
			// if (!str.contains("|E|") && in != -1) {
			// str += (char) in;
			// } else {
			// long receivedTime = System.currentTimeMillis();
			// Map<String, Long> latencies = new HashMap<String, Long>();
			// String str2 = str.substring(0, str.indexOf("|E|")).trim();
			// String[] vars = str2.split(" ");
			// for (int i = 0; i < vars.length; i += 2) {
			// if (!capturedObIds.contains(vars[i])) {
			// if (capturedObIds.remainingCapacity() <= 1) {
			// capturedObIds.poll();
			// }
			// capturedObIds.put(vars[i]);
			// // logger.info("looking for: " + vars[i]);
			// long t1 = System.currentTimeMillis();
			// long initTime = ACEISScheduler.getObMap().get(vars[i].split("#")[1]);
			// long t2 = System.currentTimeMillis();
			// if (t2 - t1 >= 100) {
			// logger.warn("Cache lookup time: " + (t2 - t1));
			// }
			// latencies.put(vars[i], (receivedTime - initTime));
			// }
			// }
			//
			// MultipleInstanceMain.getMonitor().addResults(getURI(), latencies, 1);
			// this.streamResultAsTriples(str2.split(" "));
			// str = "";
			// }
			// }
			// cw.flush();
			// cw.close();
		} catch (Exception e) {
			logger.error(" Exception: " + e.getMessage());
			// ioe.printStackTrace();
			// this.stop();
			// } catch (Exception e) {
			// logger.error("Exception: " + e.getMessage());
			// // e.printStackTrace();
			// // this.stop();
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
		// boolean isNew = false;
		// if (latestResult == null) {
		// isNew = true;
		// // latestResult = resultArray;
		// }
		int arraySize = resultArray.length;
		if (arraySize % 2 != 0)
			logger.error(this.getURI() + ": Result size incorrect. " + Arrays.asList(resultArray));
		else {
			Model m = ModelFactory.createDefaultModel();
			for (int i = 0; i < resultArray.length; i = i + 2) {
				String obIdStr = resultArray[i];
				// if (!isNew)
				// if (!obIdStr.equals(latestResult[i])) {
				// isNew = true;
				// break;
				// }
				String serviceIdStr = serviceList.get(i / 2).replaceAll("<", "").replaceAll(">", "");
				String sensorIdStr = sensorList.get(i / 2).replaceAll("<", "").replaceAll(">", "");
				String propertyIdStr = propertyList.get(i / 2).replaceAll("<", "").replaceAll(">", "");
				// String typeStr = resultArray[i + 1];
				Resource propertyId = m.createResource(propertyIdStr);
				String valueStr = resultArray[i + 1];
				Resource observation = m.createResource(obIdStr);
				Property hasValue = m.createProperty(VirtuosoDataManager.saoPrefix + "hasValue");
				observation.addProperty(RDF.type, m.createResource(VirtuosoDataManager.ssnPrefix + "Observation"));
				Resource sensorId = m.createResource(sensorIdStr);

				observation.addProperty(m.createProperty(VirtuosoDataManager.ssnPrefix + "observedBy"), sensorId);
				observation.addProperty(m.createProperty(VirtuosoDataManager.ssnPrefix + "observedProperty"),
						propertyId);
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
			// if (isNew) {
			// byteCnt += resultArray.toString().getBytes().length;
			messageCnt += 1;
			// latestResult = resultArray;

			long messageByte = 0;
			logger.debug("Result array: " + Arrays.asList(resultArray));

			byteCnt += messageByte;
			// latestResult = resultArray;

			List<Statement> stmts = m.listStatements().toList();
			try {
				for (Statement st : stmts) {
					stream(st.getSubject().asNode(), st.getPredicate().asNode(), st.getObject().asNode());
					// logger.info(this.getURI() + " Streaming Result: " + st.toString());
					// messageByte += st.toString().getBytes().length;
				}
			} catch (Exception e) {
				logger.error("Exception while sending result triples");
			}
			// if (this.session != null)
			// this.session.getBasicRemote().sendText(consoleDisplay);
			// } else {
			// logger.info("Duplicated message dicarded.");
			// }
		}

	}
}
