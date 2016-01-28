package org.insight_centre.aceis.io.rdf;

import java.io.BufferedReader;
import java.io.ByteArrayInputStream;
import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.OutputStream;
import java.net.URI;
import java.net.URL;
import java.net.URLConnection;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.UUID;

import org.deri.cqels.engine.ExecContext;
import org.deri.cqels.engine.ReasonerContext;
import org.insight_centre.aceis.eventmodel.EventDeclaration;
import org.insight_centre.aceis.eventmodel.EventOperator;
import org.insight_centre.aceis.eventmodel.EventPattern;
import org.insight_centre.aceis.eventmodel.EventRequest;
import org.insight_centre.aceis.eventmodel.Filter;
import org.insight_centre.aceis.eventmodel.MsgBusGrounding;
import org.insight_centre.aceis.eventmodel.NodeRemovalException;
import org.insight_centre.aceis.eventmodel.OperatorType;
import org.insight_centre.aceis.eventmodel.QosVector;
import org.insight_centre.aceis.eventmodel.Selection;
import org.insight_centre.aceis.eventmodel.TrafficReportService;
import org.insight_centre.aceis.eventmodel.WeightVector;
import org.insight_centre.aceis.io.EventRepository;
import org.insight_centre.aceis.utils.test.qos.StreamQualityAnalyzer;
import org.insight_centre.citypulse.commons.data.json.SensorSnapshot;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import virtuoso.jena.driver.VirtGraph;

import com.google.gson.Gson;
import com.hp.hpl.jena.ontology.OntModelSpec;
import com.hp.hpl.jena.query.Dataset;
import com.hp.hpl.jena.query.DatasetFactory;
import com.hp.hpl.jena.query.QueryExecution;
import com.hp.hpl.jena.query.QueryExecutionFactory;
import com.hp.hpl.jena.query.QuerySolution;
import com.hp.hpl.jena.query.ResultSet;
import com.hp.hpl.jena.rdf.model.Container;
import com.hp.hpl.jena.rdf.model.Literal;
import com.hp.hpl.jena.rdf.model.Model;
import com.hp.hpl.jena.rdf.model.ModelFactory;
import com.hp.hpl.jena.rdf.model.NodeIterator;
import com.hp.hpl.jena.rdf.model.Property;
import com.hp.hpl.jena.rdf.model.RDFNode;
import com.hp.hpl.jena.rdf.model.RDFWriter;
import com.hp.hpl.jena.rdf.model.Resource;
import com.hp.hpl.jena.rdf.model.Statement;
import com.hp.hpl.jena.reasoner.Reasoner;
import com.hp.hpl.jena.reasoner.ReasonerRegistry;
import com.hp.hpl.jena.sparql.core.DatasetGraph;
import com.hp.hpl.jena.sparql.core.DatasetImpl;
import com.hp.hpl.jena.tdb.TDBFactory;
import com.hp.hpl.jena.util.FileManager;
import com.hp.hpl.jena.vocabulary.RDF;

public class VirtuosoDataManager {
	private static final String cesGraphName = "http://iot.ee.surrey.ac.uk/citypulse/ontologies/ces";
	public static final String cqelsHome = "CQELS_DB/";
	public static final String databaseDirectory = "CQELS_DB/dataset/";
	public static Dataset dataset;
	private static int cqelsIndex = 0;
	public static final String datasetDirectory = "dataset/";
	public static final String defaultGraphName = "http://ict-citypulse.eu/store/citypulse/datasets/servicerepository";
	public static final String avgQualityGraphName = "http://ict-citypulse.eu/store/citypulse/avgquality";
	public static final String defaultPrefix = "http://www.insight-centre.org/dataset/SampleEventService#",
			ssnPrefix = "http://purl.oclc.org/NET/ssnx/ssn#",
			owlsPrefix = "http://www.daml.org/services/owl-s/1.2/Service.owl#",
			owlsspPrefix = "http://www.daml.org/services/owl-s/1.2/ServiceParameter.owl#",
			owlsscPrefix = "http://www.daml.org/services/owl-s/1.2/ServiceCategory.owl#",
			emvoPrefix = "http://sense.deri.ie/vocab/emvo#", muoPrefix = "http://purl.oclc.org/NET/muo/ucum/",
			ctPrefix = "http://ict-citypulse.eu/city#", drPrefix = "http://www.insight-centre.org/datarequest#",
			qPrefix = "http://purl.oclc.org/NET/UASO/qoi#", saoPrefix = "http://purl.oclc.org/NET/UNIS/sao/sao#",
			upPrefix = "http://www.ict-citypulse.eu/ontologies/userprofile#",
			cesPrefix = "http://www.insight-centre.org/ces#",
			osmPrefix = "http://www.insight-centre.org/ontologies/osm#",
			geoPrefix = "http://www.w3.org/2003/01/geo/wgs84_pos#";
	private static final Logger logger = LoggerFactory.getLogger(VirtuosoDataManager.class);
	public static String ontologyDirectory = "ontology/";

	public static final OntModelSpec ontoSpec = OntModelSpec.RDFS_MEM_RDFS_INF;
	public static final Map<String, String> prefixMap = new HashMap<String, String>();
	public static String queryPrefix = "";
	public static final String virtAcc = "dba", virtPsw = "dba";
	// public static String virtuosoURI = "http://131.227.92.55:8011/sparql";
	public static String virtuosoURI = "http://127.0.0.1:8890/sparql";
	public static final String virtUrl = "localhost:1111";
	static {
		prefixMap.put("", defaultPrefix);
		prefixMap.put("ces", cesPrefix);
		prefixMap.put("owls", owlsPrefix);
		prefixMap.put("owlssp", owlsspPrefix);
		prefixMap.put("owlssc", owlsscPrefix);
		prefixMap.put("emvo", emvoPrefix);
		prefixMap.put("muo", muoPrefix);
		prefixMap.put("ssn", ssnPrefix);
		prefixMap.put("ct", ctPrefix);
		prefixMap.put("up", upPrefix);
		prefixMap.put("geo", geoPrefix);
		prefixMap.put("sao", saoPrefix);
		prefixMap.put("rdf", "http://www.w3.org/1999/02/22-rdf-syntax-ns#");
		prefixMap.put("rdfs", "http://www.w3.org/2000/01/rdf-schema#");
		prefixMap.put("xsd", "http://www.w3.org/2001/XMLSchema#");
		prefixMap.put("q", qPrefix);
		prefixMap.put("osm", "http://www.insight-centre.org/ontologies/osm#");
		prefixMap.put("qoi", "http://ict-citypulse.eu/ontologies/StreamQoI/");
		prefixMap.put("sce1", "http://ict-citypulse.eu/experiments/Scenario1/");
		prefixMap.put("prov", "http://www.w3.org/ns/prov#");
		prefixMap.put("tl", "http://purl.org/NET/c4dm/timeline.owl#");
		prefixMap.put("xml", "http://www.w3.org/XML/1998/namespace");
		// System.out.println(prefixMap);
		for (Entry<String, String> entry : prefixMap.entrySet())
			queryPrefix = queryPrefix + " prefix " + entry.getKey() + ": <" + entry.getValue() + ">";
	}

	public static EventRepository buildRepoFromLocalFile(String uri, int simSize) throws Exception {
		if (dataset == null)
			VirtuosoDataManager.initializeDataset(uri);
		logger.info("Extracting service repository from: " + uri);
		long t1 = System.currentTimeMillis();
		EventRepository er = new EventRepository();
		Map<String, EventDeclaration> edMap = extractEDsFromDataset();
		// create clones
		List<EventDeclaration> clones = new ArrayList<EventDeclaration>();
		for (EventDeclaration ed : edMap.values()) {
			// random qos and fixed frequency for local tests
			ed.setInternalQos(QosVector.getGoodRandomQos());
			ed.setFrequency(0.3);
			for (int i = 0; i < simSize; i++) {
				EventDeclaration edClone = ed.clone();
				edClone.setnodeId(ed.getnodeId() + "-sim" + i);
				edClone.setServiceId(edClone.getnodeId());
				edClone.setInternalQos(QosVector.getGoodRandomQos());
				clones.add(edClone);
			}
		}
		for (EventDeclaration edClone : clones)
			edMap.put(edClone.getnodeId(), edClone);

		// Map<String, EventDeclaration> edMap = new HashMap<String, EventDeclaration>();
		Map<String, EventPattern> epMap = new HashMap<String, EventPattern>();
		for (EventDeclaration ed : edMap.values()) {
			// edMap.put(ed.getID(), ed);
			if (ed.getEp() != null) {
				epMap.put(ed.getEp().getID(), ed.getEp());
				if (ed.getEp().hasInconsistentSelection())
					logger.warn("Inconsistent Selection in: " + ed.getEp().toString());
			} else if (ed.getFoi().equals("") || ed.getFoi() == null)
				logger.warn("empty foi: " + ed.getServiceId());
		}

		er.setEds(edMap);
		er.setEps(epMap);

		long t2 = System.currentTimeMillis();
		logger.info("Service repository created in: " + (t2 - t1) + " ms with " + er.getEds().size() + " EDs and "
				+ er.getEps().size() + " EPs.");

		// for (EventDeclaration ed : er.getEds().values()) {
		// if (!ed.getEventType().toLowerCase().contains("traffic"))
		// logger.info("ED: " + ed);
		// }
		// RDFDataMgr.write(System.out, dataset.getNamedModel(cesPrefix), Lang.TURTLE);

		// dataset.close();
		return er;
	}

	public static EventRepository buildRepoFromSparqlEndpoint(URI uri, int simSize) throws Exception {
		if (uri != null) {
			virtuosoURI = uri.toString();
		}
		// simSize = 0;
		logger.info("Extracting service repository from: " + virtuosoURI);
		long t1 = System.currentTimeMillis();
		EventRepository er = new EventRepository();
		// if (dataset != null) {
		Map<String, EventDeclaration> edMap = extractEDsFromDataset();
		// create clones
		List<EventDeclaration> clones = new ArrayList<EventDeclaration>();
		for (EventDeclaration ed : edMap.values()) {
			ed.setFrequency(0.3);
			for (int i = 0; i < simSize; i++) {
				EventDeclaration edClone = ed.clone();
				edClone.setInternalQos(QosVector.getGoodRandomQos());
				edClone.setnodeId(ed.getnodeId() + "-sim" + i);
				edClone.setServiceId(edClone.getnodeId());
				clones.add(edClone);
			}
		}
		for (EventDeclaration edClone : clones)
			edMap.put(edClone.getnodeId(), edClone);

		Map<String, EventPattern> epMap = new HashMap<String, EventPattern>();
		for (EventDeclaration ed : edMap.values()) {
			// edMap.put(ed.getID(), ed);
			if (ed.getEp() != null) {
				epMap.put(ed.getEp().getID(), ed.getEp());
				if (ed.getEp().hasInconsistentSelection())
					logger.warn("Inconsistent Selection in: " + ed.getEp().toString());
			} else if (ed.getFoi().equals("") || ed.getFoi() == null)
				logger.warn("empty foi: " + ed.getServiceId());
		}

		er.setEds(edMap);
		er.setEps(epMap);

		long t2 = System.currentTimeMillis();
		logger.info("Service repository created in: " + (t2 - t1) + " ms with " + er.getEds().size() + " EDs and "
				+ er.getEps().size() + " EPs.");

		for (EventDeclaration ed : er.getEds().values()) {
			if (!ed.getEventType().toLowerCase().contains("traffic"))
				logger.debug("ED: " + ed);
		}
		// RDFDataMgr.write(System.out, dataset.getNamedModel(cesPrefix), Lang.TURTLE);
		// writeRepoToFile("virtGraph.n3", er);
		// dataset.close();
		return er;
	}

	private static void buildTempMap(EventPattern ep, List<String> childIds) {
		// System.out.println("building temp: ");
		for (int i = 1; i < childIds.size(); i++) {
			ep.getTemporalMap().put(childIds.get(i - 1), childIds.get(i));
		}
		// System.out.println("building temp: " + ep.getTemporalMap());
	}

	private static void cleanNCreate(String path) {
		deleteDir(new File(path));
		if (!(new File(path)).mkdir()) {
			System.out.println("can not create working directory" + path);
		}
	}

	public static void closeDataset() {
		dataset.close();
	}

	public static HashMap<String, String> getSnapShot(EventDeclaration ed) throws Exception {
		URL url = new URL(ed.getHttpGrounding());
		URLConnection yc = url.openConnection();
		BufferedReader in = new BufferedReader(new InputStreamReader(yc.getInputStream()));
		String inputLine;
		String response = "";
		while ((inputLine = in.readLine()) != null)
			response += inputLine;
		in.close();

		SensorSnapshot ss = new Gson().fromJson(response, SensorSnapshot.class);
		String message = ss.getData();
		message = message.replaceAll("\t", " ").replaceAll("\n", " ");
		System.out.println(message);
		Model m = ModelFactory.createDefaultModel();
		m.read(new ByteArrayInputStream(message.getBytes()), null, "N3");

		List<Statement> stmts = m.listStatements().toList();
		logger.info("Snapshot Statements received: " + stmts.size());

		String queryStr = queryPrefix + " select  ?pId ?value where { ?ob ssn:observedProperty ?pId. "
				+ "?ob sao:value ?value.}";
		QueryExecution qe = QueryExecutionFactory.create(queryStr, m);
		HashMap<String, String> resultMap = new HashMap<String, String>();
		ResultSet results = qe.execSelect();
		while (results.hasNext()) {
			QuerySolution qs = results.next();
			RDFNode pid = qs.get("pId");
			RDFNode value = qs.get("value");
			String queryStr2 = queryPrefix + "select ?pt where { <" + pid + "> a ?pt.}";
			QueryExecution qe2 = QueryExecutionFactory.sparqlService(virtuosoURI, queryStr2, defaultGraphName);
			String propertyType = "";
			ResultSet res = qe2.execSelect();
			while (res.hasNext()) {
				QuerySolution qs2 = res.next();
				RDFNode pt = qs2.get("pt");
				if (pt.toString().contains("Property") || pt.toString().contains("Class") || pt.toString().equals(""))
					continue;
				propertyType = pt.toString();
			}
			if (!propertyType.equals("")) {
				resultMap.put(propertyType, value.toString());
				// ed.setSrc(addr.toString());
				// System.out.println("putting: " + propertyType + ", " + value.toString());
			}
		}
		return resultMap;
	}

	public static Model createEDModel(Model m, EventDeclaration ed) throws NodeRemovalException {
		// System.out.println("Creating ed: " + ed.toString());
		// System.out.println(m.setNsPrefixes(prefixMap));
		// System.out.println(m.getNsPrefixURI(""));
		// m.setNsPrefixes(prefixMap);
		// m.setNsPrefixes(arg0)
		// Statement typeStmt;
		// m.add
		Resource edID = m.createResource(ed.getServiceId());
		// Resource
		Resource profile = m.createResource(ed.getServiceId() + "-Profile");
		// edID.addProperty(m.createProperty(prefixMap.get("rdf"), "type"), m.createResource(ssnPrefix + "Sensor")); //
		// add profile
		edID.addProperty(m.createProperty(owlsPrefix + "presents"), profile);
		// for (String s : ed.getPayloads())
		// edID.addProperty(m.createProperty(ssnPrefix + "observes"), m.createResource(s));
		profile.addProperty(RDF.type, m.createResource(cesPrefix + "EventProfile"));
		if (ed.getInternalQos() != null)
			createQoSModel(profile, m, ed);
		// if (ed.getPayloads() != null && ed.getPayloads().size() > 0)
		// createPayloadModel(edID, m, ed);
		if (ed.getSrc() != null) // add service grounding
			edID.addProperty(m.createProperty(owlsPrefix, "supports"),
					m.createResource().addProperty(RDF.type, m.createResource(cesPrefix + "HttpGrounding"))
							.addProperty(m.createProperty(cesPrefix, "httpService"), m.createResource(ed.getSrc())));
		if (ed.getMsgBusGrounding() != null) {
			edID.addProperty(
					m.createProperty(owlsPrefix, "supports"),
					m.createResource()
							.addProperty(RDF.type, m.createResource(cesPrefix + "MessageBusGrounding"))
							.addLiteral(m.createProperty(cesPrefix + "hasExchangeName"),
									ed.getMsgBusGrounding().getExchange())
							.addLiteral(m.createProperty(cesPrefix + "hasServerAddress"),
									ed.getMsgBusGrounding().getServerAddress())
							.addLiteral(m.createProperty(cesPrefix + "hasTopic"), ed.getMsgBusGrounding().getTopic()));
		}
		if (ed.getEp() == null) { // primitive event service
			edID.addProperty(RDF.type, m.createResource(cesPrefix + "PrimitiveEventService"));
			// Resource foi = m.createResource(ed.getFoi());
			String coordinateStr = ed.getFoi();
			String[] coordinates = coordinateStr.split("-");
			Double node1Lat = Double.parseDouble(coordinates[0].split(",")[0]);
			Double node1Lon = Double.parseDouble(coordinates[0].split(",")[1]);

			Double node2Lat = 0.0;
			// Double.parseDouble(coordinateStr.split("-")[1].split(",")[0]);
			Double node2Lon = 0.0;
			if (coordinates.length > 1) {
				node2Lat = Double.parseDouble(coordinates[1].split(",")[0]);
				node2Lon = Double.parseDouble(coordinates[1].split(",")[1]);
			}
			// = Double.parseDouble(coordinateStr.split("-")[1].split(",")[1]);
			// foi.addProperty(RDF.type, m.createResource(osmPrefix + "Node"));
			// if (ed instanceof TrafficReportService) {
			// edID.addProperty(RDF.type, m.createResource(ssnPrefix + "Sensor"));
			profile.addProperty(m.createProperty(owlsscPrefix + "serviceCategory"),
					m.createResource().addProperty(RDF.type, m.createResource(owlsscPrefix + "ServiceCategory"))
							.addLiteral(m.createProperty(owlsscPrefix + "serviceCategoryName"), ed.getEventType()));
			// foi = createTrafficLocation(m, ed);

			// profile.addLiteral(m.createProperty(ctPrefix + "hasReportID"),
			// ((TrafficReportService) ed).getReportId());

			// }

			boolean foiAdded = false;
			for (String s : ed.getPayloads()) {
				String type = s.split("\\|")[0];
				String foiStr = s.split("\\|")[1];
				String id = s.split("\\|")[2];
				Resource foi = m.createResource(foiStr);
				if (!foiAdded) {

					foi.addProperty(RDF.type, m.createResource(ssnPrefix + "FeatureOfInterest"));
					if (node2Lat != 0.0) {
						Resource startLoc = m.createResource();
						Resource endLoc = m.createResource();
						foi.addProperty(m.createProperty(saoPrefix + "beginsAtLocation"), startLoc);
						startLoc.addLiteral(m.createProperty(geoPrefix + "lat"), node1Lat);
						startLoc.addLiteral(m.createProperty(geoPrefix + "long"), node1Lon);
						foi.addProperty(m.createProperty(saoPrefix + "endsAtLocation"), endLoc);
						endLoc.addLiteral(m.createProperty(geoPrefix + "lat"), node2Lat);
						endLoc.addLiteral(m.createProperty(geoPrefix + "long"), node2Lon);
					} else {
						Resource loc = m.createResource();
						foi.addProperty(m.createProperty(ssnPrefix + "hasLocation"), loc);
						loc.addLiteral(m.createProperty(geoPrefix + "lat"), node1Lat);
						loc.addLiteral(m.createProperty(geoPrefix + "long"), node1Lon);
					}
					foiAdded = true;
				}
				Resource observedProperty = m.createResource(id);
				observedProperty.addProperty(RDF.type, m.createResource(type));
				observedProperty.addProperty(m.createProperty(ssnPrefix + "isPropertyOf"), foi);
				Resource sensor = m.createResource(ed.getSensorId());
				edID.addProperty(m.createProperty(cesPrefix + "hasPhysicalEventSource"), sensor);
				sensor.addProperty(RDF.type, m.createResource(ssnPrefix + "Sensor"));
				sensor.addProperty(m.createProperty(ssnPrefix + "observes"), observedProperty);
			}

		} else { // complex event service
			edID.addProperty(RDF.type, m.createResource(cesPrefix + "ComplexEventService"));
			createEPModel(profile, m, ed);
		}

		// if(ed.getInternalQos()!=null ||)
		return m;
	}

	private static void createEPModel(Resource profile, Model m, EventDeclaration ed) throws NodeRemovalException {
		if (ed.getEp() != null) {
			EventPattern ep = ed.getEp();
			String rootId = ep.getRootId();
			Resource bNode = m.createResource();
			if (profile == null)
				profile = m.createResource("DummyService-" + UUID.randomUUID() + "-Profile");
			profile.addProperty(m.createProperty(cesPrefix + "hasPattern"), bNode);
			createSelections(bNode, m, ep);
			traverseToCreate(rootId, bNode, ep, m);
		}

	}

	private static void createFilter(Resource bnode, Filter f, String cid, Model m) {
		Property hasFilter = m.createProperty(cesPrefix + "hasFilter");

		Resource filter = m.createResource();

		filter.addProperty(RDF.type, m.createResource(cesPrefix + "Filter"));
		filter.addProperty(m.createProperty(cesPrefix + "onPayload"), m.createResource(f.getVar()));
		if (cid != null) {
			// bnode = m.createResource();
			filter.addProperty(m.createProperty(cesPrefix + "onEvent"), m.createResource(cid));
		}
		bnode.addProperty(hasFilter, filter);
		Property op;
		if (f.getOp() == Filter.eq)
			op = m.createProperty(emvoPrefix + "equals");
		else if (f.getOp() == Filter.lt)
			op = m.createProperty(emvoPrefix + "lessThan");
		else if (f.getOp() == Filter.leq)
			op = m.createProperty(emvoPrefix + "lessOrEqualThan");
		else if (f.getOp() == Filter.gt)
			op = m.createProperty(emvoPrefix + "greaterThan");
		else {
			op = m.createProperty(emvoPrefix + "greaterOrEqualThan");
		}
		// Literal val = m.(f.getVal());
		if (f.getVal() instanceof String)
			filter.addLiteral(op, m.createTypedLiteral((String) f.getVal()));
		else if (f.getVal() instanceof Integer)
			filter.addLiteral(op, m.createTypedLiteral((Integer) f.getVal()));
		else
			filter.addLiteral(op, m.createTypedLiteral(Double.parseDouble(f.getVal().toString())));
	}

	private static void createQoSModel(Resource profile, Model m, EventDeclaration ed) {
		if (ed.getInternalFrequency() != null) {
			createQosParam(profile, "Frequency", ed.getInternalFrequency(), m);
		}
		if (ed.getInternalQos() != null) {
			QosVector qos = ed.getInternalQos();
			Double a = qos.getAccuracy();
			Double r = qos.getReliability();
			int l = qos.getLatency();
			int p = qos.getPrice();
			int s = qos.getSecurity();
			createQosParam(profile, "Accuracy", a, m);
			createQosParam(profile, "Reliability", r, m);
			createQosParam(profile, "Latency", l, m);
			createQosParam(profile, "Price", p, m);
			createQosParam(profile, "Security", s, m);
		}

	}

	private static void createQosParam(Resource profile, String pName, Object pValue, Model m) {
		profile.addProperty(
				m.createProperty(cesPrefix + "has" + pName),
				m.createResource()
						.addProperty(RDF.type, m.createResource(cesPrefix + pName))
						.addProperty(m.createProperty(owlsspPrefix + "serviceParameterName"), pName)
						.addProperty(
								m.createProperty(owlsspPrefix + "sParameter"),
								m.createResource()
										.addLiteral(m.createProperty(emvoPrefix + "hasQuantityValue"), pValue)));
	}

	private static void createSelections(Resource patternNode, Model m, EventPattern ep) {
		Property hasSelection = m.createProperty(cesPrefix + "hasSelection");
		Property hasNodeId = m.createProperty(cesPrefix + "hasNodeId");
		Property hasSelectedProperty = m.createProperty(cesPrefix + "selectedProperty");
		// System.out.p rintln("Creating selections:------------");
		for (Selection sel : ep.getSelections()) {
			// System.out.println("SEL: " + sel.toString());
			Resource pName = m.createResource(sel.getPropertyName());
			String nodeId = sel.getProvidedBy();
			// Resource pType = m.createResource(sel.getPropertyType());
			// EventDeclaration ed;
			// if (ep.isQuery() && sel.getOriginalED() != null)
			// ed = sel.getOriginalED();
			// Resource foi = m.createResource(sel.getFoi());
			patternNode.addProperty(
					hasSelection,
					m.createResource().addProperty(RDF.type, m.createResource(cesPrefix + "Selection"))
							.addProperty(hasNodeId, m.createLiteral(nodeId)).addProperty(hasSelectedProperty, pName));
			// if (ep.isQuery()) {// add property type and foi for query
			// pName.addProperty(RDF.type, pType).addProperty(m.createProperty(ssnPrefix + "isPropertyOf"), foi);
			// }
		}
	}

	private static Resource createTrafficLocation(Model m, EventDeclaration ed) {
		Resource foi = m.createResource(defaultPrefix + "FoI-" + UUID.randomUUID());
		foi.addProperty(RDF.type, m.createResource(ssnPrefix + "FeatureOfInterest"));
		Resource firstNode = m.createResource();
		firstNode.addProperty(RDF.type, m.createResource(ctPrefix + "Node"));
		foi.addProperty(m.createProperty(ctPrefix + "hasFirstNode"), firstNode);
		firstNode.addLiteral(m.createProperty(ctPrefix + "hasStreetNumber"),
				((TrafficReportService) ed).getNode1StreetNo());
		firstNode.addLiteral(m.createProperty(ctPrefix + "hasStreet"), ((TrafficReportService) ed).getNode1Street());
		firstNode.addLiteral(m.createProperty(ctPrefix + "hasCity"), ((TrafficReportService) ed).getNode1City());
		firstNode.addLiteral(m.createProperty(ctPrefix + "hasLatitude"), ((TrafficReportService) ed).getNode1Lat());
		firstNode.addLiteral(m.createProperty(ctPrefix + "hasLongtitude"), ((TrafficReportService) ed).getNode1Lon());
		firstNode.addLiteral(m.createProperty(ctPrefix + "hasNodeName"), ((TrafficReportService) ed).getNode1Name());

		Resource secNode = m.createResource();
		secNode.addProperty(RDF.type, ctPrefix + "Node");
		foi.addProperty(m.createProperty(ctPrefix + "hasSecondNode"), secNode);
		secNode.addLiteral(m.createProperty(ctPrefix + "hasStreetNumber"),
				((TrafficReportService) ed).getNode2StreetNo());
		secNode.addLiteral(m.createProperty(ctPrefix + "hasStreet"), ((TrafficReportService) ed).getNode2Street());
		secNode.addLiteral(m.createProperty(ctPrefix + "hasCity"), ((TrafficReportService) ed).getNode2City());
		secNode.addLiteral(m.createProperty(ctPrefix + "hasLatitude"), ((TrafficReportService) ed).getNode2Lat());
		secNode.addLiteral(m.createProperty(ctPrefix + "hasLongtitude"), ((TrafficReportService) ed).getNode2Lon());
		secNode.addLiteral(m.createProperty(ctPrefix + "hasNodeName"), ((TrafficReportService) ed).getNode2Name());
		return foi;
	}

	// private static void createPayloadModel(Resource profile, Model m, EventDeclaration ed) {
	// Resource payload = m.createResource();
	// profile.addProperty(m.createProperty(cesPrefix, "hasEventPayload"), payload);
	// for (String p : ed.getPayloads())
	// payload.addProperty(m.createProperty(ssnPrefix, "observedProperty"), m.createResource(p));
	// Resource foi;
	// if (ed instanceof TrafficReportService) {
	// foi = m.createResource();
	// foi.addProperty(RDF.type, m.createResource(ssnPrefix + "FeatureOfInterest"));
	// createTrafficLocation(foi, m, ed);
	// } else
	// foi = m.createResource(ed.getFoi());
	//
	// payload.addProperty(RDF.type, m.createResource(cesPrefix + "EventPayload"))
	// .addProperty(RDF.type, m.createResource(ssnPrefix + "Observation"))
	// .addProperty(m.createProperty(ssnPrefix, "featureOfInterest"), foi);
	//
	// }

	private static boolean deleteDir(File dir) {
		if (dir.isDirectory()) {
			String[] children = dir.list();
			for (int i = 0; i < children.length; i++) {
				boolean success = deleteDir(new File(dir, children[i]));
				if (!success) {
					System.out.println("can not delete" + dir);
					return false;
				}
			}
		}
		return dir.delete();
	}

	public static EventPattern extractCompositionPlanFromDataset(String serviceRequest) {
		Model queryBase = FileManager.get().loadModel(datasetDirectory + serviceRequest);
		dataset.getDefaultModel().add(ModelFactory.createOntologyModel(ontoSpec, queryBase));

		String describeStr = queryPrefix + " select ?x  where{?x rdf:type ces:CompositionPlan}";
		// Query query = QueryFactory.create(describeStr);
		// query.setPrefixMapping(pmap);
		QueryExecution qe = QueryExecutionFactory.create(describeStr, dataset);
		ResultSet results = qe.execSelect();
		// ResultSetFormatter.out(System.out, results, query);
		Map<String, EventDeclaration> edMap = new HashMap<String, EventDeclaration>();
		EventPattern ep = new EventPattern();
		ep.setQuery(false);
		while (results.hasNext()) {
			// System.out.println("results!");
			QuerySolution row = results.next();
			RDFNode edID = row.get("x");
			// System.out.println("has id: " + edID.toString());
			ep = extractEDByServiceID(edID, dataset, edMap).getEp();
		}
		return ep;
	}

	public static void extractConstraintAndPreferenceById(String qid, QosVector constraint, WeightVector weight) {
		qid = qid.replaceFirst("EP-", "");
		String describeStr = queryPrefix + " describe ?x where {<" + qid + "> owls:presents ?x}";
		QueryExecution qe = QueryExecutionFactory.sparqlService(virtuosoURI, describeStr, defaultGraphName);
		// sparqlService(String service, String query, String defaultGraph)
		Model queryModel = qe.execDescribe();
		// RDFDataMgr.write(System.out, queryModel, Lang.TURTLE);
		String constQueryStr = queryPrefix + " select ?x ?y where {?x rdf:type ces:Constraint. }";
		QueryExecution constQE = QueryExecutionFactory.sparqlService(virtuosoURI, constQueryStr, defaultGraphName);
		ResultSet constResults = constQE.execSelect();
		// QosVector constraint = new QosVector();
		while (constResults.hasNext()) {
			QuerySolution row = constResults.next();
			RDFNode constId = row.get("x");
			// RDFNode exp = row.get("y");
			RDFNode property = queryModel.getProperty((Resource) constId,
					queryModel.createProperty(cesPrefix + "onProperty")).getObject();
			// System.out.println("has property: " + property);
			RDFNode expression = queryModel.getProperty((Resource) constId,
					queryModel.createProperty(cesPrefix + "hasExpression")).getObject();
			// System.out.println("has expression: " + expression);
			Property comparison;
			if (queryModel.contains((Resource) expression, queryModel.createProperty(emvoPrefix + "lessThan")))
				comparison = queryModel.getProperty(emvoPrefix + "lessThan");
			else if (queryModel.contains((Resource) expression,
					queryModel.createProperty(emvoPrefix + "lessOrEqualThan")))
				comparison = queryModel.getProperty(emvoPrefix + "lessOrEqualThan");
			else if (queryModel.contains((Resource) expression, queryModel.createProperty(emvoPrefix + "greaterThan")))
				comparison = queryModel.getProperty(emvoPrefix + "greaterThan");
			else if (queryModel.contains((Resource) expression,
					queryModel.createProperty(emvoPrefix + "greaterOrEqualThan")))
				comparison = queryModel.getProperty(emvoPrefix + "greaterOrEqualThan");
			else
				comparison = queryModel.getProperty(emvoPrefix + "equals");
			// System.out.println("has comparison: " + comparison);
			Object value = queryModel.getProperty((Resource) expression, comparison).getLiteral().getValue();
			if (property.toString().equals(cesPrefix + "Correctness"))
				constraint.setAccuracy((Double) value);
			else if (property.toString().equals(cesPrefix + "Reliability"))
				constraint.setReliability((Double) value);
			else if (property.toString().equals(cesPrefix + "Security"))
				constraint.setSecurity((Integer) value);
			else if (property.toString().equals(cesPrefix + "Price"))
				constraint.setPrice((Integer) value);
			else if (property.toString().equals(cesPrefix + "Latency"))
				constraint.setLatency((Integer) value);
		}
		String prefQueryStr = queryPrefix + " select ?x where {?x rdf:type ces:QosWeightPreference}";
		QueryExecution prefQE = QueryExecutionFactory.create(prefQueryStr, queryModel);
		ResultSet prefResults = prefQE.execSelect();
		// QosVector constraint = new QosVector();
		while (prefResults.hasNext()) {
			QuerySolution row = prefResults.next();
			RDFNode prefId = row.get("x");
			Double availabilityW = queryModel.getProperty((Resource) prefId,
					queryModel.createProperty(cesPrefix + "availabilityWeight")).getDouble();
			Double reliabilityW = queryModel.getProperty((Resource) prefId,
					queryModel.createProperty(cesPrefix + "reliabilityWeight")).getDouble();
			Double priceW = queryModel.getProperty((Resource) prefId,
					queryModel.createProperty(cesPrefix + "priceWeight")).getDouble();
			Double securityW = queryModel.getProperty((Resource) prefId,
					queryModel.createProperty(cesPrefix + "securityWeight")).getDouble();
			Double latencyW = queryModel.getProperty((Resource) prefId,
					queryModel.createProperty(cesPrefix + "latencyWeight")).getDouble();
			weight.setAccuracyW(availabilityW);
			weight.setLatencyW(latencyW);
			weight.setPriceW(priceW);
			weight.setSecurityW(securityW);
			weight.setReliabilityW(reliabilityW);
		}
	}

	private static QosVector extractConstraintFromModel(Model queryModel) throws Exception {
		QosVector constraint = QosVector.getLooseConstraint();
		String constQueryStr = queryPrefix + " select ?x ?y where {?x rdf:type ces:Constraint. }";
		QueryExecution constQE = QueryExecutionFactory.create(constQueryStr, queryModel);

		ResultSet constResults = constQE.execSelect();
		// QosVector constraint = new QosVector();
		while (constResults.hasNext()) {
			QuerySolution row = constResults.next();
			RDFNode constId = row.get("x");
			// RDFNode exp = row.get("y");
			RDFNode property = queryModel.getProperty((Resource) constId,
					queryModel.createProperty(cesPrefix + "onParameter")).getObject();
			// System.out.println("has property: " + property);
			RDFNode expression = queryModel.getProperty((Resource) constId,
					queryModel.createProperty(cesPrefix + "hasExpression")).getObject();
			// System.out.println("has expression: " + expression);
			Property comparison;
			if (queryModel.contains((Resource) expression, queryModel.createProperty(emvoPrefix + "lessThan")))
				comparison = queryModel.getProperty(emvoPrefix + "lessThan");
			else if (queryModel.contains((Resource) expression,
					queryModel.createProperty(emvoPrefix + "lessOrEqualThan")))
				comparison = queryModel.getProperty(emvoPrefix + "lessOrEqualThan");
			else if (queryModel.contains((Resource) expression, queryModel.createProperty(emvoPrefix + "greaterThan")))
				comparison = queryModel.getProperty(emvoPrefix + "greaterThan");
			else if (queryModel.contains((Resource) expression,
					queryModel.createProperty(emvoPrefix + "greaterOrEqualThan")))
				comparison = queryModel.getProperty(emvoPrefix + "greaterOrEqualThan");
			else
				comparison = queryModel.getProperty(emvoPrefix + "equals");
			// System.out.println("has comparison: " + comparison);
			Object value = queryModel.getProperty((Resource) expression, comparison).getLiteral().getValue();
			if (property.toString().equals(qPrefix + "Correctness"))
				constraint.setAccuracy((Double) value);
			else if (property.toString().equals(qPrefix + "Reliability"))
				constraint.setReliability((Double) value);
			else if (property.toString().equals(qPrefix + "Security"))
				constraint.setSecurity((Integer) value);
			else if (property.toString().equals(qPrefix + "Price"))
				constraint.setPrice((Integer) value);
			else if (property.toString().equals(qPrefix + "Latency"))
				constraint.setLatency((Integer) value);
			else if (property.toString().equals(qPrefix + "NetworkConsumption"))
				constraint.setTraffic((Double) value);
			else
				throw new Exception("Constraint un-recognized.");

		}
		return constraint;
	}

	private static EventDeclaration extractEDByServiceID(RDFNode edID, Dataset dataset,
			Map<String, EventDeclaration> edMap) {
		if (!edMap.containsKey(edID)) {
			// logger.info("Extracting service: " + edID);
			EventDeclaration ed;
			String type = extractEventType(edID.toString(), dataset);
			if (type.toLowerCase().contains("traffic")) {
				ed = new TrafficReportService(edID.toString(), "", type, null, null, null);
				// extractTrafficLocation(ed, dataset);
			} else {

				ed = new EventDeclaration(edID.toString(), "", type, null, null, null, null);
				return null;
			}

			extractEventSource(ed, dataset);
			String foiId = extractEventPayloads(ed, dataset);
			// extractEventQoSFromSeparateGraph(ed);
			extractEventQoS(ed, dataset);
			extractEventFoI(ed, dataset, foiId);
			extractReportId(ed, dataset);
			// logger.info("addr: " + ed.getMsgBusGrounding().getServerAddress() + ", exchange: "
			// + ed.getMsgBusGrounding().getExchange() + ", topic: " + ed.getMsgBusGrounding().getTopic());
			// extractEventFoIAsResource(ed, dataset, foiId);
			try {
				extractEventPattern(ed, dataset, edMap);
			} catch (CloneNotSupportedException e) {
				logger.error("ACEIS IO: Pattern extraction failed.");
				e.printStackTrace();
			}
			edMap.put(edID.toString(), ed);
			logger.debug("Extracted: " + ed.toString());
			return ed;
		}
		return edMap.get(edID);
	}

	// private static void createFilterForEventOperator(Resource bnode, Filter f, Model m) {
	// Property hasFilter = m.createProperty(cesPrefix + "hasFilter");
	// Resource filter = m.createResource();
	// filter.addProperty(RDF.type, m.createResource(cesPrefix + "Filter"));
	// bnode.addProperty(hasFilter, filter);
	// filter.addProperty(m.createProperty(cesPrefix + "onPayload"), m.createResource(f.getVar()));
	// Property op;
	// if (f.getOp() == Filter.eq)
	// op = m.createProperty(emvoPrefix + "equals");
	// else if (f.getOp() == Filter.lt)
	// op = m.createProperty(emvoPrefix + "lessThan");
	// else if (f.getOp() == Filter.leq)
	// op = m.createProperty(emvoPrefix + "lessOrEqualThan");
	// else if (f.getOp() == Filter.gt)
	// op = m.createProperty(emvoPrefix + "greaterThan");
	// else {
	// op = m.createProperty(emvoPrefix + "greaterOrEqualThan");
	// }
	// if (f.getVal() instanceof Double)
	// filter.addLiteral(op, m.createTypedLiteral((Double) f.getVal()));
	// else if (f.getVal() instanceof Integer)
	// filter.addLiteral(op, m.createTypedLiteral((Integer) f.getVal()));
	// else
	// filter.addLiteral(op, m.createTypedLiteral((String) f.getVal()));
	// }

	private static Map<String, EventDeclaration> extractEDsFromDataset() throws IOException, NodeRemovalException {
		// List<EventDeclaration> eds = new ArrayList<EventDeclaration>();
		QueryExecution qe;
		String describeStr; // Query query = QueryFactory.create(describeStr);
		// query.setPrefixMapping(pmap);
		if (dataset != null) {
			logger.info("dataset not null.");
			describeStr = queryPrefix + " select ?s" + " where{?s a ces:PrimitiveEventService.}";
			qe = QueryExecutionFactory.create(describeStr, dataset);
		} else {
			// with ontology
			// describeStr = queryPrefix + "select ?s from <http://iot.ee.surrey.ac.uk/citypulse/ontologies/ces> "
			// + "where { ?s a ?st." + "graph <http://iot.ee.surrey.ac.uk/citypulse/ontologies/ces> "
			// + "{?st rdfs:subClassOf ces:EventService}}";

			// without ontology
			describeStr = queryPrefix + "select ?s where {?s a ces:PrimitiveEventService.}";
			qe = QueryExecutionFactory.sparqlService(virtuosoURI, describeStr, defaultGraphName);

		}
		ResultSet results = qe.execSelect();
		// logger.info("has results: " + results.hasNext());
		// ResultSetFormatter.out(System.out, results, query);
		Map<String, EventDeclaration> edMap = new HashMap<String, EventDeclaration>();
		int i = 0;
		while (results.hasNext()) {
			QuerySolution row = results.next();
			RDFNode edID = row.get("s");
			// if (edID.toString().equals(
			// "http://ict-citypulse.eu/PrimitiveEventService-8289e77a-952e-5424-b9f9-4fcf3efff0f5")
			// || edID.toString().equals(
			// "http://ict-citypulse.eu/PrimitiveEventService-2e0f6b77-71a9-560d-8256-52a71bc0475a")
			// || edID.toString().equals(
			// "http://ict-citypulse.eu/PrimitiveEventService-b994900e-1e0a-5141-b916-c76e06b40ebe")
			// || edID.toString().equals(
			// "http://ict-citypulse.eu/PrimitiveEventService-d7c01a1d-2266-5058-a818-5fb8cceb5ba7"))
			// System.out.println("has id: " + edID.toString());
			logger.info("Extracting ED No. " + i);
			extractEDByServiceID(edID, dataset, edMap);
			i++;
			// logger.info("Extracted service: " + edID + ", rid: " + edMap.get(edID.toString()).getRid() + ", sid: "
			// + edMap.get(edID.toString()).getSensorId());
		}
		// List<EventDeclaration> eds = new ArrayList<EventDeclaration>();
		// for (EventDeclaration ed : edMap.values())
		// eds.add(ed);
		// writeEDsToFile("VirtGraph.n3", eds);
		// RDFDataMgr.write(System.out, results, Lang.TURTLE);
		return edMap;
	}

	// private static Map<String, EventDeclaration> extractEDsFromEndpoint() {
	// // List<EventDeclaration> eds = new ArrayList<EventDeclaration>();
	// String describeStr = queryPrefix
	// + " select ?x"
	// + " where{?x rdf:type ?y graph <http://www.insight-centre.org/ces#> { ?y rdfs:subClassOf ces:EventService}}";
	// // Query query = QueryFactory.create(describeStr);
	// // query.setPrefixMapping(pmap);
	// QueryExecution qe = QueryExecutionFactory.create(describeStr, dataset);
	// ResultSet results = qe.execSelect();
	// // ResultSetFormatter.out(System.out, results, query);
	// Map<String, EventDeclaration> edMap = new HashMap<String, EventDeclaration>();
	// while (results.hasNext()) {
	// QuerySolution row = results.next();
	// RDFNode edID = row.get("x");
	// // System.out.println("has id: " + edID.toString());
	// extractEDByServiceID(edID, dataset, edMap);
	// }
	// // RDFDataMgr.write(System.out, results, Lang.TURTLE);
	// return edMap;
	// }

	private static void extractEventFoI(EventDeclaration ed, Dataset dataset, String foiId) {
		// if (ed instanceof TrafficReportService) {
		// extractTrafficLocation(ed, dataset);
		// } else {
		String foiStr = extractFoIById(dataset, foiId);
		// ed.setFoi(foiStr);
		if (foiStr != null)
			ed.setFoi(foiStr);
		else
			extractFoIAsResource(ed, dataset, foiId);
		// for (String payload : ed.getPayloads())
		// payload = payload.replace(foiId, foiStr);
	}

	private static void extractEventPattern(EventDeclaration ed, Dataset dataset, Map<String, EventDeclaration> edMap)
			throws CloneNotSupportedException {
		String desribeStr = queryPrefix + " describe  ?z where {<" + ed.getnodeId()
				+ "> owls:presents ?y. ?y ces:hasPattern ?z.}";
		QueryExecution qe1;
		if (dataset != null)
			qe1 = QueryExecutionFactory.create(desribeStr, dataset);
		else
			qe1 = QueryExecutionFactory.sparqlService(virtuosoURI, desribeStr, defaultGraphName);
		Model patternModel = qe1.execDescribe();
		// RDFDataMgr.write(System.out, patternModel, Lang.TURTLE);
		EventPattern ep = new EventPattern();
		ep.setID("EP-" + ed.getnodeId());

		// ep.setFilters(new HashMap<String, List<Filter>>());
		String queryRoot = queryPrefix + " select  ?z where {<" + ed.getnodeId()
				+ "> owls:presents ?y. ?y ces:hasPattern ?z.}";
		QueryExecution qe2;
		if (dataset != null) {
			// logger.info("extracting local ep. ");
			qe2 = QueryExecutionFactory.create(queryRoot, dataset);
		} else
			qe2 = QueryExecutionFactory.sparqlService(virtuosoURI, queryRoot, defaultGraphName);
		ResultSet results = qe2.execSelect();
		// logger.info("extracting ep: " + ep.getID());
		if (results.hasNext()) {
			ed.setEp(ep);
			// logger.info("extracting ep2: " + ep.getID());
			RDFNode root = results.next().get("z");
			traverseToExtract(0, root, patternModel, ep, dataset, edMap);
			extractSelectionMap(root, patternModel, ep, dataset, edMap);
			// System.out.println("EP extracted: " + ep.toString());
		}
		// traverse
		// RDFDataMgr.write(System.out, results, Lang.TURTLE);

	}

	private static String extractEventPayloads(EventDeclaration ed, Dataset dataset) {
		String queryStr = queryPrefix
				+ " select ?x ?y ?z ?sensor where {<"
				+ ed.getnodeId()
				+ "> ces:hasPhysicalEventSource ?sensor. ?sensor ssn:observes ?x. ?x rdf:type ?y. ?x ssn:isPropertyOf ?z }";
		QueryExecution qe;
		if (dataset != null)
			qe = QueryExecutionFactory.create(queryStr, dataset);
		else
			qe = QueryExecutionFactory.sparqlService(virtuosoURI, queryStr, defaultGraphName);
		ResultSet results = qe.execSelect();
		List<String> payloads = new ArrayList<String>();
		String foiId = "";
		String sensorId = "";
		while (results.hasNext()) {
			QuerySolution result = results.next();
			RDFNode propertyName = result.get("x");
			RDFNode propertyType = result.get("y");
			RDFNode foi = result.get("z");
			sensorId = result.get("sensor").toString();
			if (propertyType.toString().equals("http://www.w3.org/2000/01/rdf-schema#Resource"))
				continue;
			// System.out.println("type: " + property + " foi: " + foi);
			payloads.add(propertyType.toString() + "|" + foi.toString() + "|" + propertyName.toString());
			foiId = foi.toString();
			// System.out.println("payload: " + propertyType.toString() + "|" + foi.toString() + "|"
			// + propertyName.toString());
		}
		ed.setSensorId(sensorId);
		ed.setPayloads(payloads);
		return foiId;
	}

	private static void extractEventQoSFromSeparateGraph(EventDeclaration ed) {
		// logger.info("extracting qos for: " + ed.getServiceId());
		String queryStr = queryPrefix + " select  ?profile where {<" + ed.getServiceId() + "> owls:presents ?profile.}";
		QueryExecution qe = QueryExecutionFactory.sparqlService(virtuosoURI, queryStr, defaultGraphName);
		ResultSet rs1 = qe.execSelect();
		String profile = "";
		if (rs1.hasNext()) {
			profile = rs1.next().get("profile").toString();
		}
		if (!profile.equals("")) {
			// logger.info("extracting qos for: " + profile);
			String queryStr2 = queryPrefix + " select ?qt ?qav ?qrv where {<" + profile
					+ "> q:hasQuality ?qid. ?qid a ?qt. ?qid q:hasAbsoluteQuality ?qav. ?qid q:hasRatedQuality ?qrv.}";
			QueryExecution qe2 = QueryExecutionFactory.sparqlService(virtuosoURI, queryStr2, avgQualityGraphName);
			ResultSet rs2 = qe2.execSelect();
			QosVector qos = QosVector.getGoodRandomQos();
			ed.setInternalQos(qos);
			while (rs2.hasNext()) {
				QuerySolution qs2 = rs2.next();
				String qt = qs2.get("qt").toString();
				String qav = qs2.get("qav").toString();
				String qrv = qs2.get("qrv").toString();
				if (qav.contains("None") || qrv.contains("None")) {
					// logger.info("skipping qos analysis.");
					continue;
				}
				if (qt.contains("Latency")) {
					double latency_second = Double.parseDouble(qav);
					qos.setLatency((int) (latency_second * 1000.0));
					// logger.info("latency: " + latency_second);
				} else if (qt.contains("Correctness")) {
					double correctness = Double.parseDouble(qav);
					qos.setAccuracy(correctness);
					// logger.info("correctness: " + correctness);
				} else if (qt.contains("Completeness")) {
					double completeness = Double.parseDouble(qrv);
					qos.setReliability(completeness);
					// logger.info("completeness: " + completeness);
				}
			}
		}
	}

	private static void extractEventQoS(EventDeclaration ed, Dataset dataset) {
		// TODO change quality prefix

		String queryStr = queryPrefix
				+ " select  ?type ?value where {<"
				+ ed.getnodeId()
				+ "> owls:presents ?profile. ?profile ?hasnfp ?x. ?x rdf:type ?type. ?x owlssp:sParameter ?y. ?y emvo:hasQuantityValue ?value "
				+ " graph <http://www.insight-centre.org/ces#> {?hasnfp rdfs:subPropertyOf owlssp:serviceParameter}}";

		// String queryStr = queryPrefix
		// +
		// " select ?srvId ?type ?value where {?srvId owls:presents ?profile. ?profile ?hasnfp ?x. ?x rdf:type ?type. ?x owlssp:sParameter ?y. ?y emvo:hasQuantityValue ?value }";
		long t1 = System.currentTimeMillis();
		// String queryStr = queryPrefix
		// + "select ?srvId ?srvCatName ?pType ?foiId ?a ?b ?c ?d ?qType ?qValue where { "
		// +
		// "?srvId owls:presents ?profile. ?profile owlssc:serviceCategory ?srvCat. ?srvCat owlssc:serviceCategoryName "
		// + "?srvCatName. ?srvId ssn:observes ?pid. ?pid rdf:type ?pType. ?pid ssn:isPropertyOf ?foiId. "
		// + "?foiId ct:hasStartLatitude ?a. ?foiId ct:hasStartLongitude ?b. ?foiId ct:hasEndLatitude ?c. "
		// + "?foiId ct:hasEndLongitude ?d. OPTIONAL {?srvId owls:presents ?profile.  ?profile ?hasnfp ?x.  "
		// + "?x rdf:type ?qType.  ?x owlssp:sParameter ?y.  ?y emvo:hasQuantityValue ?qValue "
		// + "graph <http://www.insight-centre.org/ces#> {?hasnfp rdfs:subPropertyOf ces:hasNFP}}}";
		QueryExecution qe;
		if (dataset != null)
			qe = QueryExecutionFactory.create(queryStr, dataset);
		else
			qe = QueryExecutionFactory.sparqlService(virtuosoURI, queryStr, defaultGraphName);

		ResultSet results = qe.execSelect();
		QosVector qos = QosVector.getGoodRandomQos();
		boolean hasQos = false;
		int sum = 0;
		while (results.hasNext()) {
			sum++;
			hasQos = true;
			QuerySolution row = results.next();
			RDFNode type = row.get("type");
			RDFNode value = row.get("value");

			// ed.setSrc(addr.toString());
			// System.out.println("type: " + type + "\n value: " + value);
			if (type.toString().equals("http://www.insight-centre.org/ces#Security"))
				qos.setSecurity(value.asLiteral().getInt());
			else if (type.toString().equals("http://www.insight-centre.org/ces#Price"))
				qos.setPrice(value.asLiteral().getInt());
			else if (type.toString().equals("http://www.insight-centre.org/ces#Accuracy"))
				qos.setAccuracy(value.asLiteral().getDouble());
			else if (type.toString().equals("http://www.insight-centre.org/ces#Reliability"))
				qos.setReliability(value.asLiteral().getDouble());
			else if (type.toString().equals("http://www.insight-centre.org/ces#Latency"))
				qos.setLatency(value.asLiteral().getInt());
			else if (type.toString().equals("http://www.insight-centre.org/ces#BandWidthConsumption"))
				;
			else if (type.toString().equals("http://www.insight-centre.org/ces#Availability"))
				;
			else if (type.toString().equals("http://www.insight-centre.org/ces#EnergyConsumption"))
				;
			else if (type.toString().equals("http://www.insight-centre.org/ces#Frequency"))
				ed.setFrequency(value.asLiteral().getDouble());
		}
		long t2 = System.currentTimeMillis();
		// logger.info("QoS extracted: " + qos);
		if (qos.getAccuracy() != null)
			ed.setInternalQos(qos);

	}

	public static EventRequest extractEventRequestFromFile(String serviceRequest) throws Exception {
		EventRequest result = null;
		Model queryBase = ModelFactory.createOntologyModel(ontoSpec, FileManager.get().loadModel(serviceRequest));
		// dataset.getDefaultModel().add(ModelFactory.createOntologyModel(ontoSpec, queryBase));
		QosVector constraint = extractConstraintFromModel(queryBase);
		// logger.info("constraint: " + constraint.toString());
		WeightVector weight = extractPreferenceFromModel(queryBase);
		// logger.info("weight: " + weight.toString());
		Dataset dataset = DatasetFactory.create(queryBase);
		String describeStr = queryPrefix + " select ?x  where{?x rdf:type ces:EventRequest}";
		// Query query = QueryFactory.create(describeStr);
		// query.setPrefixMapping(pmap);
		QueryExecution qe = QueryExecutionFactory.create(describeStr, dataset);
		ResultSet results = qe.execSelect();
		// ResultSetFormatter.out(System.out, results, query);
		Map<String, EventDeclaration> edMap = new HashMap<String, EventDeclaration>();
		EventPattern ep = new EventPattern();
		ep.setQuery(true);
		if (results.hasNext()) {
			// System.out.println("results!");
			QuerySolution row = results.next();
			RDFNode edID = row.get("x");
			// System.out.println("has id: " + edID.toString());
			ep = extractEDByServiceID(edID, dataset, edMap).getEp();
			logger.info("event pattern: " + ep.toSimpleString());
		}
		result = new EventRequest(ep, constraint, weight);
		return result;
	}

	private static void extractEventSource(EventDeclaration ed, Dataset dataset) {
		ed.setServiceId(ed.getnodeId());
		String queryStr = queryPrefix + " select  ?z where {<" + ed.getnodeId()
				+ "> owls:supports ?y. ?y ces:httpService ?z.}";
		QueryExecution qe;
		if (dataset != null)
			qe = QueryExecutionFactory.create(queryStr, dataset);
		else
			qe = QueryExecutionFactory.sparqlService(virtuosoURI, queryStr, defaultGraphName);

		ResultSet results = qe.execSelect();
		if (results.hasNext()) {
			RDFNode addr = results.next().get("z");
			ed.setSrc(addr.toString());
			ed.setHttpGrounding(addr.toString());
			// System.out.println("addr: " + addr);
		}
		String msgbusQueryStr = queryPrefix + " select  * where {<" + ed.getnodeId()
				+ "> owls:supports ?y. ?y ces:hasExchangeName ?exchange."
				+ "?y ces:hasServerAddress ?serverAddr. ?y ces:hasTopic ?topic.}";
		QueryExecution qe2;
		if (dataset != null)
			qe2 = QueryExecutionFactory.create(msgbusQueryStr, dataset);
		else
			qe2 = QueryExecutionFactory.sparqlService(virtuosoURI, msgbusQueryStr, defaultGraphName);

		ResultSet results2 = qe2.execSelect();
		if (results2.hasNext()) {
			QuerySolution qs = results2.next();
			RDFNode addr = qs.get("serverAddr");
			RDFNode exchange = qs.get("exchange");
			RDFNode topic = qs.get("topic");
			MsgBusGrounding grounding = new MsgBusGrounding();
			grounding.setExchange(exchange.asLiteral().getString());
			// grounding.setExchange("");
			// hard-coded exchange
			grounding.setExchange("annotated_data");
			grounding.setServerAddress(addr.asLiteral().getString());
			// grounding.setServerAddress("amqp://guest:guest@131.227.92.55:8009");
			grounding.setTopic(topic.asLiteral().getString());
			ed.setMsgBusGrounding(grounding);
			// logger.info("add");
		}
	}

	private static String extractEventType(String edID, Dataset dataset) {
		String queryStr = queryPrefix + " select  ?name where {<" + edID
				+ "> owls:presents ?profile. ?profile owlssc:serviceCategory ?z. ?z owlssc:serviceCategoryName ?name}";
		QueryExecution qe;
		if (dataset != null) {
			qe = QueryExecutionFactory.create(queryStr, dataset);
		} else {
			qe = QueryExecutionFactory.sparqlService(virtuosoURI, queryStr, defaultGraphName);
		}
		ResultSet results = qe.execSelect();
		String type = "";
		if (results.hasNext()) {
			Literal l = results.next().getLiteral("name");
			type = l.getString();
		}
		return type;
	}

	private static Map<String, List<Filter>> extractFilterMap(String nodeId, List<RDFNode> filters, Model m,
			EventPattern ep) {
		Map<String, List<Filter>> results = new HashMap<String, List<Filter>>();
		// if (!nodeId.equals(""))
		// results.put(nodeId, new ArrayList<Filter>());
		for (RDFNode filterNode : filters) {
			// RDFNode filterNode = filters.next();
			RDFNode payload = m.getProperty((Resource) filterNode, m.createProperty(cesPrefix + "onPayload"))
					.getObject();
			int op = 0;
			Object val;
			Property lt = m.createProperty(emvoPrefix + "lessThan");
			Property leq = m.createProperty(emvoPrefix + "lessOrEqualThan");
			Property gt = m.createProperty(emvoPrefix + "greaterThan");
			Property geq = m.createProperty(emvoPrefix + "greaterOrEqualThan");
			Property eq = m.createProperty(emvoPrefix + "equals");
			if (m.contains((Resource) filterNode, lt)) {
				op = Filter.lt;
				val = m.getProperty((Resource) filterNode, lt).getLiteral().getValue();
			} else if (m.contains((Resource) filterNode, leq)) {
				op = Filter.leq;
				val = m.getProperty((Resource) filterNode, leq).getLiteral().getValue();
			} else if (m.contains((Resource) filterNode, gt)) {
				op = Filter.gt;
				val = m.getProperty((Resource) filterNode, gt).getLiteral().getValue();
			} else if (m.contains((Resource) filterNode, geq)) {
				op = Filter.geq;
				val = m.getProperty((Resource) filterNode, geq).getLiteral().getValue();
			} else {
				op = Filter.eq;
				val = m.getProperty((Resource) filterNode, eq).getLiteral().getValue();
			}
			Filter f = new Filter(payload.toString(), val, op);
			// System.out.println("filter: " + f);
			// if (!nodeId.equals(""))
			// results.get(nodeId).add(f);
			// else {
			if (m.getProperty((Resource) filterNode, m.createProperty(cesPrefix + "onEvent")) != null) {
				RDFNode eventId = m.getProperty((Resource) filterNode, m.createProperty(cesPrefix + "onEvent"))
						.getObject();
				// if (eventId != null) {
				if (results.get(eventId) == null)
					results.put(eventId.toString(), new ArrayList<Filter>());
				results.get(eventId.toString()).add(f);
				// if(ep.getNodeById(eventId.toString())==null)
				// ep.getEds().add(extractEDByID(eventId))
			} else {
				if (results.get(nodeId) == null)
					results.put(nodeId, new ArrayList<Filter>());
				results.get(nodeId).add(f);
			}
			// }
		}
		return results;
	}

	private static void extractFoIAsResource(EventDeclaration ed, Dataset dataset, String foiId) {
		// if (ed instanceof TrafficReportService) {
		// extractTrafficLocation(ed, dataset);
		// } else {
		String queryStr = queryPrefix + " select  ?foi where { <" + ed.getServiceId()
				+ "> ces:hasPhysicalEventSource ?sensor. ?sensor ssn:observes ?p. ?p ssn:isPropertyOf ?foi.}";
		QueryExecution qe;
		if (dataset != null)
			qe = QueryExecutionFactory.create(queryStr, dataset);
		else
			qe = QueryExecutionFactory.sparqlService(virtuosoURI, queryStr, defaultGraphName);
		ResultSet results = qe.execSelect();
		if (results.hasNext()) {
			RDFNode foi = results.next().get("foi");
			ed.setFoi(foi.toString());
			// }
		}
	}

	private static String extractFoIById(Dataset dataset, String foiId) {

		String queryStr = queryPrefix + " select  ?a ?b ?c ?d where { <" + foiId + "> sao:beginsAtLocation ?startLoc."
				+ " ?startLoc geo:lat ?a.  ?startLoc geo:long ?b. optional{<" + foiId
				+ "> sao:endsAtLocation ?endLoc. ?endLoc geo:lat ?c. ?endLoc geo:long ?d.}}";
		QueryExecution qe;
		if (dataset != null)
			qe = QueryExecutionFactory.create(queryStr, dataset);
		else
			qe = QueryExecutionFactory.sparqlService(virtuosoURI, queryStr, defaultGraphName);
		ResultSet results = qe.execSelect();
		if (results.hasNext()) {
			// RDFNode foi = results.next().get("y");
			QuerySolution qs = results.next();
			Double startLat = qs.getLiteral("a").getDouble();
			Double startLon = qs.getLiteral("b").getDouble();
			Double endLat = qs.getLiteral("c").getDouble();
			Double endLon = qs.getLiteral("d").getDouble();
			// logger.info("extracted foi: " + startLat + "," + startLon + "-" + endLat + "," + endLon);
			return startLat + "," + startLon + "-" + endLat + "," + endLon;
			// ed.setFoi(startLat + "," + startLon + "-" + endLat + "," + endLon);
			// }
		}

		String queryStr2 = queryPrefix + " select  ?a ?b where { <" + foiId + "> ssn:hasLocation ?startLoc."
				+ " ?startLoc geo:lat ?a.  ?startLoc geo:long ?b. }";
		QueryExecution qe2;
		if (dataset != null)
			qe2 = QueryExecutionFactory.create(queryStr2, dataset);
		else
			qe2 = QueryExecutionFactory.sparqlService(virtuosoURI, queryStr2, defaultGraphName);
		ResultSet results2 = qe2.execSelect();
		if (results2.hasNext()) {
			// RDFNode foi = results.next().get("y");
			QuerySolution qs = results2.next();
			Double lat = qs.getLiteral("a").getDouble();
			Double longitude = qs.getLiteral("b").getDouble();
			logger.debug("extracted foi: " + lat + "," + longitude);
			return lat + "," + longitude;
			// ed.setFoi(startLat + "," + startLon + "-" + endLat + "," + endLon);
			// }
		}
		return null;

	}

	private static WeightVector extractPreferenceFromModel(Model queryModel) {
		WeightVector weight = WeightVector.getEvenWeights();
		String prefQueryStr = queryPrefix + " select ?x where {?x rdf:type ces:QosWeightPreference}";
		QueryExecution prefQE = QueryExecutionFactory.create(prefQueryStr, queryModel);
		ResultSet prefResults = prefQE.execSelect();
		// QosVector constraint = new QosVector();
		while (prefResults.hasNext()) {
			QuerySolution row = prefResults.next();
			RDFNode prefId = row.get("x");
			Property correctnessWeightProperty = queryModel.createProperty(cesPrefix + "correctnessWeight");
			if (queryModel.getProperty((Resource) prefId, correctnessWeightProperty) != null)
				weight.setAccuracyW(queryModel.getProperty((Resource) prefId, correctnessWeightProperty).getDouble());

			Property reliabilityW = queryModel.createProperty(cesPrefix + "reliabilityWeight");
			if (queryModel.getProperty((Resource) prefId, reliabilityW) != null)
				weight.setReliabilityW(queryModel.getProperty((Resource) prefId, reliabilityW).getDouble());

			Property priceW = queryModel.createProperty(cesPrefix + "priceWeight");
			if (queryModel.getProperty((Resource) prefId, priceW) != null)
				weight.setPriceW(queryModel.getProperty((Resource) prefId, priceW).getDouble());

			Property securityW = queryModel.createProperty(cesPrefix + "securityWeight");
			if (queryModel.getProperty((Resource) prefId, securityW) != null)
				weight.setSecurityW(queryModel.getProperty((Resource) prefId, securityW).getDouble());

			Property latencyW = queryModel.createProperty(cesPrefix + "latencyWeight");
			if (queryModel.getProperty((Resource) prefId, latencyW) != null)
				weight.setLatencyW(queryModel.getProperty((Resource) prefId, latencyW).getDouble());

			Property trafficW = queryModel.createProperty(cesPrefix + "networkConsumptionWeight");
			if (queryModel.getProperty((Resource) prefId, trafficW) != null)
				weight.setTrafficW(queryModel.getProperty((Resource) prefId, trafficW).getDouble());

		}
		return weight;
	}

	private static void extractReportId(EventDeclaration ed, Dataset dataset) {
		String queryStr = queryPrefix + " select  ?sid where {<" + ed.getnodeId()
				+ "> ces:hasPhysicalEventSource ?y. ?y prov:hadPrimarySource ?sid.}";
		QueryExecution qe;
		if (dataset != null)
			qe = QueryExecutionFactory.create(queryStr, dataset);
		else
			qe = QueryExecutionFactory.sparqlService(virtuosoURI, queryStr, defaultGraphName);

		ResultSet results = qe.execSelect();
		while (results.hasNext()) {
			RDFNode sid = results.next().get("sid");
			if (!sid.toString().contains("ReportID"))
				continue;
			String rid = sid.toString().split("-")[sid.toString().split("-").length - 1];
			for (String eid : StreamQualityAnalyzer.idMap.keySet())
				if (StreamQualityAnalyzer.idMap.get(eid).equals(rid)) {
					ed.setEid(eid);
					ed.setRid(rid);
					// logger.info("ED: " + ed.getnodeId() + " eid: " + eid + ", rid: " + rid);
					break;
				}
			// String eid = StreamQualityAnalyzer.idMap.get(rid);
		}

	}

	// public static EventPattern extractQueryFromModel(Model serviceRequest) {
	// // Model queryBase = FileManager.get().loadModel(datasetDirectory + serviceRequest);
	// dataset.getDefaultModel().add(ModelFactory.createOntologyModel(ontoSpec, serviceRequest));
	//
	// String describeStr = queryPrefix + " select ?x  where{?x rdf:type ces:EventRequest}";
	// // Query query = QueryFactory.create(describeStr);
	// // query.setPrefixMapping(pmap);
	// QueryExecution qe = QueryExecutionFactory.create(describeStr, dataset);
	// ResultSet results = qe.execSelect();
	// // ResultSetFormatter.out(System.out, results, query);
	// Map<String, EventDeclaration> edMap = new HashMap<String, EventDeclaration>();
	// EventPattern ep = new EventPattern();
	//
	// while (results.hasNext()) {
	// // System.out.println("results!");
	// QuerySolution row = results.next();
	// RDFNode edID = row.get("x");
	// // System.out.println("has id: " + edID.toString());
	// ep = extractEDByServiceID(edID, dataset, edMap).getEp();
	// }
	// return ep;
	// }

	private static void extractSelectionMap(RDFNode root, Model patternModel, EventPattern ep, Dataset dataset,
			Map<String, EventDeclaration> edMap) throws CloneNotSupportedException {

		List<RDFNode> selections = patternModel.listObjectsOfProperty((Resource) root,
				patternModel.createProperty(cesPrefix + "hasSelection")).toList();
		if (selections.size() > 0) {
			ep.setSelections(new ArrayList<Selection>());
			for (RDFNode selection : selections) {
				RDFNode property = patternModel.getProperty((Resource) selection,
						patternModel.createProperty(cesPrefix + "selectedProperty")).getObject();
				// System.out.println("extracting property: " + property.toString());
				RDFNode foi = dataset
						.getDefaultModel()
						.listObjectsOfProperty((Resource) property,
								patternModel.createProperty(ssnPrefix + "isPropertyOf")).toList().get(0);
				List<RDFNode> types = dataset.getDefaultModel().listObjectsOfProperty((Resource) property, RDF.type)
						.toList();
				String typeStr = "";
				for (RDFNode type : types) {
					if (!type.toString().equals("http://www.w3.org/2000/01/rdf-schema#Resource"))
						typeStr = type.toString();
				}
				String foiStr = extractFoIById(dataset, foi.toString());
				// String type = "";
				// for (String s : payloads) {
				// if (s.split("\\|")[2].equals(property.toString())) {
				// type = s.split("\\|")[0];
				// foi = s.split("\\|")[1];
				// break;
				// }
				// }
				String nodeId = dataset.getDefaultModel()
						.getProperty((Resource) selection, patternModel.createProperty(cesPrefix + "hasNodeId"))
						.getString();
				// RDFNode
				RDFNode serviceID = patternModel.createResource(((EventDeclaration) ep.getNodeById(nodeId))
						.getServiceId());
				EventDeclaration ed;
				if (ep.isQuery())
					ed = (EventDeclaration) ep.getNodeById(nodeId);
				else {
					ed = extractEDByServiceID(serviceID, dataset, edMap);
					if (ed.getEp() != null) {
						List<EventDeclaration> eds = ed.getEp().clone().getCompletePattern().getEds();
						List<Resource> serviceIDs = dataset
								.getDefaultModel()
								.listSubjectsWithProperty(patternModel.createProperty(ssnPrefix + "observes"),
										(Resource) property).toList();
						boolean found = false;
						for (EventDeclaration ed2 : eds) {
							for (Resource r : serviceIDs) {
								if (ed2.getServiceId().equals(r.toString())) {
									ed = ed2;
									found = true;
									break;
								}
							}
							if (found)
								break;
						}
						if (!found)
							logger.error("Cannot find original ED for property: " + property.toString());
					}
				}
				Selection sel = new Selection(property.toString(), nodeId, ed, foiStr, typeStr);
				ep.getSelections().add(sel);
				// if (ep.getSelectionMap().get(serviceID.toString()) == null) {
				// ep.getSelectionMap().put(serviceID.toString(), new ArrayList<String>());
				// }
				// ep.getSelectionMap().get(serviceID.toString()).add(type.toString());
			}
		}
	}

	private static void extractTrafficLocation(EventDeclaration ed, Dataset dataset) {
		// String queryStr = queryPrefix
		// + " select ?rid ?fnn ?fnsn ?fnst ?fnc ?fnlat ?fnlon ?snn ?snsn ?snst ?snc ?snlat ?snlon where {<"
		// + ed.getID()
		// + "> owls:presents ?profile. <"
		// + ed.getID()
		// +
		// "> ssn:observes ?y. ?profile ct:hasReportID ?rid. ?y ssn:isPropertyOf ?z. ?z ct:hasFirstNode ?fn. ?z ct:hasSecondNode ?sn. "
		// +
		// " ?fn ct:hasNodeName ?fnn. ?fn ct:hasStreetNumber ?fnsn. ?fn ct:hasStreet ?fnst. ?fn ct:hasCity ?fnc. ?fn ct:hasLatitude ?fnlat. ?fn ct:hasLongtitude ?fnlon."
		// +
		// " ?sn ct:hasNodeName ?snn. ?sn ct:hasStreetNumber ?snsn. ?sn ct:hasStreet ?snst. ?sn ct:hasCity ?snc. ?sn ct:hasLatitude ?snlat. ?sn ct:hasLongtitude ?snlon.}";
		String queryStr = queryPrefix + " select ?z where { <" + ed.getnodeId()
				+ "> ssn:observes ?y.   ?y ssn:isPropertyOf ?z.}  ";
		// +
		// " ?fn ct:hasNodeName ?fnn. ?fn ct:hasStreetNumber ?fnsn. ?fn ct:hasStreet ?fnst. ?fn ct:hasCity ?fnc. ?fn ct:hasLatitude ?fnlat. ?fn ct:hasLongtitude ?fnlon."
		// +
		// " ?sn ct:hasNodeName ?snn. ?sn ct:hasStreetNumber ?snsn. ?sn ct:hasStreet ?snst. ?sn ct:hasCity ?snc. ?sn ct:hasLatitude ?snlat. ?sn ct:hasLongtitude ?snlon.}";
		QueryExecution qe = QueryExecutionFactory.create(queryStr, dataset);
		ResultSet results = qe.execSelect();
		if (results.hasNext()) {
			QuerySolution row = results.next();
			String foiStr = row.get("z").toString();
			// String reportId = row.get("rid").asLiteral().getString();
			// String firstNodeName = row.get("fnn").asLiteral().getString();
			// String firstNodeStreetNo = row.get("fnsn").asLiteral().getString();
			// String firstNodeStreet = row.get("fnst").asLiteral().getString();
			// String firstNodeCity = row.get("fnc").asLiteral().getString();
			// Double firstNodeLat = row.get("fnlat").asLiteral().getDouble();
			// Double firstNodeLon = row.get("fnlon").asLiteral().getDouble();
			//
			// String secondNodeName = row.get("snn").asLiteral().getString();
			// String secondNodeStreetNo = row.get("snsn").asLiteral().getString();
			// String secondNodeStreet = row.get("snst").asLiteral().getString();
			// String secondNodeCity = row.get("snc").asLiteral().getString();
			// Double secondNodeLat = row.get("snlat").asLiteral().getDouble();
			// Double secondNodeLon = row.get("snlon").asLiteral().getDouble();
			// ((TrafficReportService) ed).setReportId(reportId);
			// ((TrafficReportService) ed).setNode1City(firstNodeCity);
			// ((TrafficReportService) ed).setNode1Lat(firstNodeLat);
			// ((TrafficReportService) ed).setNode1Lon(firstNodeLon);
			// ((TrafficReportService) ed).setNode1Street(firstNodeStreet);
			// ((TrafficReportService) ed).setNode1StreetNo(firstNodeStreetNo);
			// ((TrafficReportService) ed).setNode1Name(firstNodeName);
			// ((TrafficReportService) ed).setNode2City(secondNodeCity);
			// ((TrafficReportService) ed).setNode2Lat(secondNodeLat);
			// ((TrafficReportService) ed).setNode2Lon(secondNodeLon);
			// ((TrafficReportService) ed).setNode2Name(secondNodeName);
			// ((TrafficReportService) ed).setNode2Street(secondNodeStreet);
			// ((TrafficReportService) ed).setNode2StreetNo(secondNodeStreetNo);
			ed.setFoi(foiStr);
		}
	}

	public static ResultSet getDataResponseFromFile(String filePath) {
		return null;

	}

	public static ExecContext initializeCQELSContext(String serviceDesc, Reasoner r) {
		ExecContext context;
		String fn = cqelsHome + cqelsIndex + "/";
		new File(fn).mkdir();
		cqelsIndex += 1;
		if (r != null)
			context = new ReasonerContext(fn, true, ReasonerRegistry.getRDFSReasoner());
		else
			context = new ExecContext(fn, true);
		context.loadDefaultDataset(datasetDirectory + serviceDesc);
		context.loadDataset(cesPrefix, ontologyDirectory + "ces.n3");
		// context.loadDataset(ssnPrefix, ontologyDirectory + "ssn.owl");
		// context.loadDataset(ctPrefix, ontologyDirectory + "city.n3");
		// context.loadDataset(owlsPrefix, ontologyDirectory + "Service.owl");
		// context.loadDataset(owlsPrefix, ontologyDirectory + "Grounding.owl");
		// context.loadDataset(owlsPrefix, ontologyDirectory + "Process.owl");
		// context.loadDataset(owlsPrefix, ontologyDirectory + "Profile.owl");

		dataset = context.getDataset().toDataset();
		return context;
	}

	// public static Dataset initializeCSPARQLContext(String serviceDesc, Reasoner r) {
	// // ExecContext context;
	// // if (r != null)
	// // context = new ReasonerContext(cqelsHome, true, ReasonerRegistry.getRDFSReasoner());
	// // else
	// // context = new ExecContext(cqelsHome, true);
	// Model defaultModel = FileManager.get().loadModel(datasetDirectory + serviceDesc);
	// Model ces = FileManager.get().loadModel(ontologyDirectory + "ces.n3");
	//
	// dataset = DatasetFactory.create(defaultModel);
	// dataset.addNamedModel(cesPrefix, ces);
	// return dataset;
	// }

	public static void initializeDataset() {
		dataset = TDBFactory.createDataset(databaseDirectory);
		// } else
		// dataset = TDBFactory.createDataset(databaseDirectory);
	}

	public static void initializeDataset(String serviceDesc) {
		// if (clean) {
		deleteDir(new File(databaseDirectory));
		if (!(new File(databaseDirectory)).mkdir()) {
			System.out.println("can not create working directory" + databaseDirectory);
		}
		DatasetGraph datasettdb = TDBFactory.createDatasetGraph(databaseDirectory);
		dataset = DatasetImpl.wrap(datasettdb);
		loadOntology(dataset);
		Model serviceBase = FileManager.get().loadModel(datasetDirectory + serviceDesc);
		dataset.getDefaultModel().add(ModelFactory.createOntologyModel(ontoSpec, serviceBase));
		// } else
		// dataset = TDBFactory.createDataset(databaseDirectory);
	}

	public static void initializeDatasetFromVirtuoso(String url, String acc, String psw) {
		dataset = DatasetFactory.create();
		// Model defaultModel = ModelFactory.createDefaultModel();
		VirtGraph vg = new VirtGraph("", url, acc, psw);
		// defaultModel.
		// ExtendedIterator<Triple> it = vg.find(Node.ANY, Node.ANY, Node.ANY);
		// for (; it.hasNext();)
		dataset.getDefaultModel().add(ModelFactory.createModelForGraph(vg));
		loadOntology(dataset);
	}

	private static void loadOntology(Dataset dataset) {
		Model ssnBase = FileManager.get().loadModel(ontologyDirectory + "ssn.owl");
		Model ssnInf = ModelFactory.createOntologyModel(ontoSpec, ssnBase);
		dataset.addNamedModel(ssnPrefix, ssnInf);

		Model owlService = FileManager.get().loadModel(ontologyDirectory + "Service.owl");
		Model owlServiceInf = ModelFactory.createOntologyModel(ontoSpec, owlService);
		dataset.addNamedModel(owlsPrefix, owlServiceInf);

		Model owlGrounding = FileManager.get().loadModel(ontologyDirectory + "Grounding.owl");
		Model owlGroundingInf = ModelFactory.createOntologyModel(ontoSpec, owlGrounding);
		dataset.addNamedModel(owlsPrefix, owlGroundingInf);

		Model owlProcess = FileManager.get().loadModel(ontologyDirectory + "Process.owl");
		Model owlProcessInf = ModelFactory.createOntologyModel(ontoSpec, owlProcess);
		dataset.addNamedModel(owlsPrefix, owlProcessInf);

		Model owlProfile = FileManager.get().loadModel(ontologyDirectory + "Profile.owl");
		Model owlProfileInf = ModelFactory.createOntologyModel(ontoSpec, owlProfile);
		dataset.addNamedModel(owlsPrefix, owlProfileInf);

		Model cesBase = FileManager.get().loadModel(ontologyDirectory + "ces.n3");
		Model cesInf = ModelFactory.createOntologyModel(ontoSpec, cesBase);
		dataset.addNamedModel(cesPrefix, cesInf);

		// Model ctBase = FileManager.get().loadModel(ontologyDirectory + "city.n3");
		// Model ctInf = ModelFactory.createOntologyModel(ontoSpec, ctBase);
		// dataset.addNamedModel(ctPrefix, ctInf);

		// FileManager.get().r(dataset.getNamedModel(ssnPrefix),
		// ModelFactory.createInfModel(ReasonerRegistry.getRDFSReasoner(), ssnBase));
		// FileManager.get().readModel(dataset.getNamedModel(owlPrefix), ontologyDirectory + "Service.owl");
		//
		// FileManager.get().readModel(dataset.getNamedModel(owlPrefix), ontologyDirectory + "Grounding.owl");
		// FileManager.get().readModel(dataset.getNamedModel(owlPrefix), ontologyDirectory + "Process.owl");
		// FileManager.get().readModel(dataset.getNamedModel(owlPrefix), ontologyDirectory + "Profile.owl");
		// FileManager.get().readModel(dataset.getNamedModel(cesPrefix), ontologyDirectory + "ces.n3");
	}

	public static void main(String[] args) throws Exception {
		// RDFFileManager.initializeDataset("Scenario1Sensors.n3");
		// uncomment next line to load from virtuoso;
		// RDFFileManager.initializeDatasetFromVirtuoso(url, acc, psw)
		// EventRepository er = VirtuosoDataManager.buildRepoFromSparqlEndpoint(null, 0);
		// VirtuosoDataManager.extractEventRequestFromFile("examples/EventRequest.n3");
		// for (Entry<String, EventDeclaration> entry : er.getEds().entrySet()) {
		// System.out.println("ed: " + entry.getValue());
		// System.out.println("edSRC: " + entry.getValue().getSrc());
		// }
		// RDFFileManager.writeRepoToFile("Scenario1Sensors-2.n3", er);
		// EventPattern query = RDFFileManager.extractQueryFromDataset("SampleEventRequest.n3");
		// QosVector constraint = new QosVector();
		// WeightVector weight = new WeightVector();
		// RDFFileManager.extractConstraintAndPreferenceById(query.getID(), constraint, weight);
		// RDFFileManager.closeDataset();

		// URL url = new URL("http://131.227.92.55:8017/api/snapshot?uuid=bcf90dc7-9da6-51a7-8b8e-da831a9918c8");
		// URLConnection yc = url.openConnection();
		// BufferedReader in = new BufferedReader(new InputStreamReader(yc.getInputStream()));
		// String inputLine;
		// String response = "";
		// while ((inputLine = in.readLine()) != null)
		// response += inputLine;
		// in.close();
		//
		// SensorSnapshot ss = new Gson().fromJson(response, SensorSnapshot.class);
		// String message = ss.getData();
		// message = message.replaceAll("\t", " ").replaceAll("\n", " ");
		// System.out.println(message);
		// Model m = ModelFactory.createDefaultModel();
		// m.read(new ByteArrayInputStream(message.getBytes()), null, "N3");
		//
		// List<Statement> stmts = m.listStatements().toList();
		// logger.info("Snapshot Statements received: " + stmts.size());
		//
		// String queryStr = queryPrefix + " select  ?pId ?value where { ?ob ssn:observedProperty ?pId. "
		// + "?ob sao:value ?value.}";
		// QueryExecution qe = QueryExecutionFactory.create(queryStr, m);
		// HashMap<String, String> resultMap = new HashMap<String, String>();
		// ResultSet results = qe.execSelect();
		// while (results.hasNext()) {
		// QuerySolution qs = results.next();
		// RDFNode pid = qs.get("pId");
		// RDFNode value = qs.get("value");
		// String queryStr2 = queryPrefix + "select ?pt where { <" + pid + "> a ?pt.}";
		// QueryExecution qe2 = QueryExecutionFactory.sparqlService("http://131.227.92.55:8011/sparql", queryStr2,
		// defaultGraphName);
		// String propertyType = "";
		// ResultSet res = qe2.execSelect();
		// while (res.hasNext()) {
		// QuerySolution qs2 = res.next();
		// RDFNode pt = qs2.get("pt");
		// if (pt.toString().contains("Property") || pt.toString().contains("Class") || pt.toString().equals(""))
		// continue;
		// propertyType = pt.toString();
		// }
		// if (!propertyType.equals("")) {
		// resultMap.put(propertyType, value.toString());
		// // ed.setSrc(addr.toString());
		// // System.out.println("putting: " + propertyType + ", " + value.toString());
		// }
		// }
		// return resultMap;

		String describeStr = queryPrefix + "construct {?s ?p ?o} where {?s ?p ?o.}";
		QueryExecution qe = QueryExecutionFactory.sparqlService("http://131.227.92.55:8011/sparql", describeStr,
				defaultGraphName);

		Model serviceModel = qe.execConstruct();
		RDFWriter writer = serviceModel.getWriter("N3");
		OutputStream out = new FileOutputStream(datasetDirectory + "bulkVirtGraph.n3");
		writer.write(serviceModel, out, null);
		out.close();
	}

	private static void traverseToCreate(String rootId, Resource bnode, EventPattern ep, Model m)
			throws NodeRemovalException {
		Object node = ep.getNodeById(rootId);
		// Property hasSubPattern = m.createProperty(cesPrefix + "hasSubPattern");
		if (node instanceof EventDeclaration) {
			bnode.addProperty(RDF.type, m.createResource(cesPrefix + "ServiceNode"));
			bnode.addProperty(m.createProperty(cesPrefix + "hasService"),
					m.createResource(((EventDeclaration) node).getServiceId()));
			bnode.addLiteral(m.createProperty(cesPrefix + "hasNodeId"), ((EventDeclaration) node).getnodeId());
			if (ep.getFilters() != null) {
				if (ep.getFilters().get(((EventDeclaration) node).getnodeId()) != null) {
					for (Filter f : ep.getFilters().get(((EventDeclaration) node).getnodeId()))
						createFilter(bnode, f, ((EventDeclaration) node).getnodeId(), m);
				}
			}
			// bnode = m.createResource(((EventDeclaration) node).getID());
		} else {
			EventOperator eo = (EventOperator) node;
			List<String> childIds = ep.getChildIds(eo.getID());
			boolean isSeq = false;
			if (eo.getOpt() == OperatorType.seq) {
				bnode.addProperty(RDF.type, m.createResource(cesPrefix + "Sequence"));
				bnode.addProperty(RDF.type, RDF.Seq);
				ep.sortSequenceChilds(childIds);
				isSeq = true;
			} else if (eo.getOpt() == OperatorType.rep) {
				bnode.addProperty(RDF.type, m.createResource(cesPrefix + "Repetition"));
				bnode.addProperty(RDF.type, RDF.Seq);
				bnode.addLiteral(m.createProperty(cesPrefix + "hasCardinality"), eo.getCardinality());
				ep.sortSequenceChilds(childIds);
				isSeq = true;
			} else if (eo.getOpt() == OperatorType.and) {
				bnode.addProperty(RDF.type, m.createResource(cesPrefix + "And"));
				bnode.addProperty(RDF.type, RDF.Bag);
			} else if (eo.getOpt() == OperatorType.or) {
				bnode.addProperty(RDF.type, m.createResource(cesPrefix + "Or"));
				bnode.addProperty(RDF.type, RDF.Bag);
			}
			if (ep.getFilters() != null) {
				if (ep.getFilters().get(eo.getID()) != null) {
					for (Filter f : ep.getFilters().get(eo.getID()))
						createFilter(bnode, f, null, m);
				}
			}
			Container container;
			if (isSeq)
				container = m.getSeq(bnode);
			else
				container = m.getBag(bnode);
			for (int i = 0; i < childIds.size(); i++) {
				String cid = childIds.get(i);
				Resource cnode;// need to write rdf:Seq for sequence and repetition
				// if (ep.getNodeById(cid) instanceof EventOperator || ep.hasFilterOn(cid))
				cnode = m.createResource();
				// else
				// cnode = m.createResource(cid);
				container.add(cnode);
				traverseToCreate(cid, cnode, ep, m);

				// } else {
				// if (ep.getFilters() != null) {
				// if (ep.getFilters().get(cid) != null) {
				// for (Filter f : ep.getFilters().get(eo.getID()))
				// createFilterForEventDeclaration(bnode, f, cid, m);
				// }
				// }
				// bnode.addProperty(hasSubPattern, m.createResource(cid));
				// }
			}
		}

	}

	private static String traverseToExtract(int eoCnt, RDFNode root, Model patternModel, EventPattern ep,
			Dataset dataset, Map<String, EventDeclaration> edMap) throws CloneNotSupportedException {

		List nodeTypes = patternModel.listObjectsOfProperty((Resource) root, RDF.type).toList();
		RDFNode nodeType = patternModel.createResource();
		boolean isOp = true;
		if (nodeTypes.contains(patternModel.getResource(cesPrefix + "And")))
			nodeType = patternModel.getResource(cesPrefix + "And");
		else if (nodeTypes.contains(patternModel.getResource(cesPrefix + "Or")))
			nodeType = patternModel.getResource(cesPrefix + "Or");
		else if (nodeTypes.contains(patternModel.getResource(cesPrefix + "Sequence")))
			nodeType = patternModel.getResource(cesPrefix + "Sequence");
		else if (nodeTypes.contains(patternModel.getResource(cesPrefix + "Repetition")))
			nodeType = patternModel.getResource(cesPrefix + "Repetition");
		else
			isOp = false;
		// = nodeTypeStmt.getObject();
		if (isOp) {// is an event operator
			// nodeType = stmt.getObject();
			// System.out.println(nodeType.toString());
			EventOperator eo = new EventOperator(OperatorType.seq, 1, ep.getID() + "-" + eoCnt);
			ep.getEos().add(eo);
			ep.getProvenanceMap().put(eo.getID(), new ArrayList<String>());
			// eoCnt += 1;
			if (nodeType.toString().equals("http://www.insight-centre.org/ces#And")) {
				eo.setOpt(OperatorType.and);
			} else if (nodeType.toString().equals("http://www.insight-centre.org/ces#Or")) {
				eo.setOpt(OperatorType.or);
			} else if (nodeType.toString().equals("http://www.insight-centre.org/ces#Repetition")) {
				eo.setOpt(OperatorType.rep);
				Property hasCard = patternModel.getProperty(cesPrefix + "hasCardinality");
				int card = patternModel.getProperty((Resource) root, hasCard).getInt();
				eo.setCardinality(card);
			}

			Property hasFilter = patternModel.getProperty(cesPrefix, "hasFilter");
			NodeIterator filters = patternModel.listObjectsOfProperty((Resource) root, hasFilter);
			Map filterMap = extractFilterMap(eo.getID(), filters.toList(), patternModel, ep);
			if (filterMap.size() > 0) {
				if (ep.getFilters() == null)
					ep.setFilters(new HashMap());
				ep.getFilters().putAll(filterMap);
			}
			boolean isSeq = nodeType.toString().equals("http://www.insight-centre.org/ces#Repetition")
					|| nodeType.toString().equals("http://www.insight-centre.org/ces#Sequence");
			Container container;
			if (isSeq)
				container = patternModel.getSeq((Resource) root);
			else
				container = patternModel.getBag((Resource) root);
			NodeIterator childNodes = container.iterator();
			List<String> childIds = new ArrayList<String>();
			while (childNodes.hasNext()) {
				Resource node = childNodes.next().asResource();
				List<RDFNode> typeList;
				if (dataset != null)
					typeList = dataset.getDefaultModel().listObjectsOfProperty(node, RDF.type).toList();
				else {
					String queryStr = queryPrefix + " select ?type where {<" + node.toString() + "> a ?type}";
					QueryExecution qe = QueryExecutionFactory.sparqlService(virtuosoURI, queryStr, defaultGraphName);
					ResultSet results = qe.execSelect();
					typeList = new ArrayList<RDFNode>();
					while (results.hasNext()) {
						RDFNode type = results.next().get("type");
						typeList.add(type);
					}
				}
				// String selectStr = queryPrefix + " ask {<" + node.toString() + "> rdf:type ces:ServiceNode.}";
				// QueryExecution qe = QueryExecutionFactory.create(selectStr, dataset);
				if (typeList.contains(patternModel.getResource(cesPrefix + "ServiceNode"))) {// is an event service node
					Property hasServiceId = patternModel.createProperty(cesPrefix + "hasService");
					Property hasNodeId = patternModel.createProperty(cesPrefix + "hasNodeId");
					RDFNode serviceId = patternModel.getProperty(node, hasServiceId).getObject();
					String nodeId;
					if (dataset != null)
						nodeId = dataset.getDefaultModel().getProperty(node, hasNodeId).getObject().asLiteral()
								.getString();
					else {
						nodeId = "";
						String queryStr = queryPrefix + " select ?nodeId where {<" + node.toString() + "> <"
								+ hasNodeId + "> ?nodeId}";
						QueryExecution qe = QueryExecutionFactory
								.sparqlService(virtuosoURI, queryStr, defaultGraphName);
						ResultSet results = qe.execSelect();
						typeList = new ArrayList<RDFNode>();
						if (results.hasNext()) {
							Literal nodeIdLiteral = results.next().get("nodeId").asLiteral();
							nodeId = nodeIdLiteral.getString();
						}
					}
					EventDeclaration childED = extractEDByServiceID(serviceId, dataset, edMap).clone();
					childED.setnodeId(nodeId);
					ep.getEds().add(childED);
					ep.getProvenanceMap().get(eo.getID()).add(nodeId);
					// ep.getServiceMap().put(nodeId, childED);
					childIds.add(nodeId);
					// System.out.println(node + " is ed");
				} else {
					eoCnt += 1;
					String childOpId = traverseToExtract(eoCnt, node, patternModel, ep, dataset, edMap);
					if (childOpId != null) {
						childIds.add(childOpId);
						ep.getProvenanceMap().get(eo.getID()).add(childOpId);
					}
				}
			}
			if (isSeq)
				buildTempMap(ep, childIds);
			// }
			return eo.getID();
		} else {

			Property hasFilter = patternModel.getProperty(cesPrefix, "hasFilter");
			List filters = patternModel.listObjectsOfProperty((Resource) root, hasFilter).toList();
			Map filterMap = extractFilterMap("", filters, patternModel, ep);
			if (filterMap.size() > 0) {
				if (ep.getFilters() == null)
					ep.setFilters(new HashMap());
				ep.getFilters().putAll(filterMap);
			}
			Property onServceNode = patternModel.getProperty(cesPrefix, "onServceNode");
			RDFNode serviceNode = patternModel.getProperty((Resource) filters.get(0), onServceNode).getObject();
			RDFNode service = patternModel.getProperty((Resource) serviceNode,
					patternModel.getProperty(cesPrefix + "hasService")).getObject();
			String serviceNodeId = patternModel
					.getProperty((Resource) serviceNode, patternModel.getProperty(cesPrefix + "hasNodeId")).getObject()
					.asLiteral().getString();
			EventDeclaration ed = extractEDByServiceID(service, dataset, edMap).clone();
			ed.setnodeId(serviceNodeId);
			ep.getEds().add(ed);
			// ep.getServiceMap().put(serviceNodeId, ed);
			return ed.getnodeId();
		}

	}

	public static void writeEDsToFile(String fileName, List<EventDeclaration> list) throws IOException,
			NodeRemovalException {
		Model serviceModel = ModelFactory.createDefaultModel();
		serviceModel.setNsPrefixes(prefixMap);
		int i = 0;
		for (EventDeclaration ed : list) {
			// System.out.println("Creating ed " + i + ":" + ed.toString());
			serviceModel.add(createEDModel(serviceModel, ed));
			i++;
		}
		RDFWriter writer = serviceModel.getWriter("N3");
		OutputStream out = new FileOutputStream(fileName);
		writer.write(serviceModel, out, null);
		out.close();
	}

	public static void writeEPsToFile(String fileName, List<EventPattern> eps) throws NodeRemovalException, IOException {
		Model serviceModel = ModelFactory.createDefaultModel();
		serviceModel.setNsPrefixes(prefixMap);
		int i = 0;
		for (EventPattern ep : eps) {
			String sid = "ED-" + UUID.randomUUID();
			EventDeclaration ed = new EventDeclaration(sid, sid, "complex", ep, null, null);
			// ed.setServiceId("ED-" + UUID.randomUUID());
			System.out.println("Creating ep " + i + ":" + ep.toSimpleString());
			createEPModel(null, serviceModel, ed);
			for (EventDeclaration ed2 : ep.getEds())
				createEDModel(serviceModel, ed2);
			i++;
		}
		RDFWriter writer = serviceModel.getWriter("N3");
		OutputStream out = new FileOutputStream(fileName);
		writer.write(serviceModel, out, null);
		out.close();
	}

	public static void writeEPToFile(String fileName, EventPattern ep) throws NodeRemovalException, IOException {
		Model serviceModel = ModelFactory.createDefaultModel();
		serviceModel.setNsPrefixes(prefixMap);
		int i = 0;
		// for (EventPattern ep : eps) {
		String sid = "ED-" + UUID.randomUUID();
		EventDeclaration ed = new EventDeclaration(sid, sid, "complex", ep, null, null);
		// ed.setServiceId("ED-" + UUID.randomUUID());
		System.out.println("Creating ep " + i + ":" + ep.toSimpleString());
		createEPModel(null, serviceModel, ed);
		for (EventDeclaration ed2 : ep.getEds())
			createEDModel(serviceModel, ed2);
		i++;
		// }
		RDFWriter writer = serviceModel.getWriter("N3");
		OutputStream out = new FileOutputStream(fileName);
		writer.write(serviceModel, out, null);
		out.close();
	}

	public static void writeRepoToFile(String fileName, EventRepository repo) throws IOException, NodeRemovalException {
		Model serviceModel = ModelFactory.createDefaultModel();
		serviceModel.setNsPrefixes(prefixMap);
		int i = 0;
		System.gc();
		for (EventDeclaration ed : repo.getEds().values()) {
			System.out.println("Creating ed " + i + ":" + ed.toString());
			serviceModel.add(createEDModel(serviceModel, ed));
			i++;

		}
		// BulkWritesr
		RDFWriter writer = serviceModel.getWriter("N3");
		OutputStream out = new FileOutputStream(datasetDirectory + fileName);
		writer.write(serviceModel, out, null);
		out.close();
	}
}
