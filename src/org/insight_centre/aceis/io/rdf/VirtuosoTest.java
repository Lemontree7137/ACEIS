package org.insight_centre.aceis.io.rdf;

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

import virtuoso.jena.driver.VirtGraph;
import virtuoso.jena.driver.VirtuosoQueryExecution;
import virtuoso.jena.driver.VirtuosoQueryExecutionFactory;

import com.csvreader.CsvWriter;
import com.hp.hpl.jena.graph.Triple;
import com.hp.hpl.jena.query.Query;
import com.hp.hpl.jena.query.QueryExecution;
import com.hp.hpl.jena.query.QueryExecutionFactory;
import com.hp.hpl.jena.query.QueryFactory;
import com.hp.hpl.jena.query.QuerySolution;
import com.hp.hpl.jena.query.ResultSet;
import com.hp.hpl.jena.rdf.model.Statement;
import com.hp.hpl.jena.rdf.model.StmtIterator;

public class VirtuosoTest {
	private static final String aliVirtuosuoURL = "jdbc:virtuoso://deri-srvgal35.nuig.ie:1111";
	private static final String sefkiVirtuosuoURL = "jdbc:virtuoso://131.227.92.55:1111";
	public static List<Long> times = new ArrayList<Long>();
	// private static final String sefkiVirtuosoURI = "http://http://131.227.92.55:8890/sparql";
	private static final String defaultGraphName = "http://iot.ee.surrey.ac.uk/citypulse/datasets/servicerepositoryNT";

	private void loadDataset() throws SQLException {
		RDFFileManager.initializeDataset("jws/SimRepo-9.n3");
		// Query sparql = QueryFactory.create("select * where{?s ?p ?o}");
		VirtGraph graph1 = new VirtGraph(RDFFileManager.defaultPrefix, sefkiVirtuosuoURL, "dba", "iotest");
		graph1.clear();
		// graph1.
		StmtIterator it = RDFFileManager.dataset.getDefaultModel().listStatements();
		List<Statement> list = it.toList();
		// int size = list.size();
		List<Triple> triples1 = new ArrayList<Triple>();
		// long sum = 0;
		for (Statement st : list) {
			// sum++;
			// System.out.println("adding triple # " + sum + " / " + size);
			// graph1.add(st.asTriple());
			triples1.add(st.asTriple());
		}
		System.out.println("adding triples to default...");
		graph1.getBulkUpdateHandler().add(triples1);
		System.out.println("graph.getCount() = " + graph1.getCount());
		graph1.close();
		// graph1.getConnection().close();
		Iterator<String> nameit = RDFFileManager.dataset.listNames();
		for (; nameit.hasNext();) {
			String graphName = nameit.next();
			System.out.println("graph: " + graphName);
			VirtGraph graph2 = new VirtGraph(graphName, aliVirtuosuoURL, "dba", "dba");
			graph2.clear();
			StmtIterator it2 = RDFFileManager.dataset.getNamedModel(graphName).listStatements();
			for (; it2.hasNext();) {
				graph2.add(it2.next().asTriple());
			}
			graph2.close();
			// graph2.getConnection().close();
		}
	}

	private class UserRequest implements Runnable {
		@Override
		public void run() {
			VirtGraph graph1 = new VirtGraph("http://www.insight-centre.org/dataset/SampleEventService#",
					"jdbc:virtuoso://deri-srvgal35.nuig.ie:1111", "dba", "dba");
			Query sparql = QueryFactory.create("prefix rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#> "
					+ "prefix emvo: <http://sense.deri.ie/vocab/emvo#> "
					+ "prefix owlssp: <http://www.daml.org/services/owl-s/1.2/ServiceParameter.owl#> "
					+ "prefix owls: <http://www.daml.org/services/owl-s/1.2/Service.owl#> "
					+ "prefix rdfs: <http://www.w3.org/2000/01/rdf-schema#> "
					+ "prefix ces: <http://www.insight-centre.org/ces#> " + "select ?serviceId ?type ?value where { "
					+ "?serviceId owls:presents ?profile.  ?profile ?hasnfp ?x.  ?x rdf:type ?type.  "
					+ "?x owlssp:sParameter ?y.  ?y emvo:hasQuantityValue ?value }");
			VirtuosoQueryExecution vqe = VirtuosoQueryExecutionFactory.create(sparql, graph1);
			long t1 = System.currentTimeMillis();
			ResultSet rs = vqe.execSelect();
			writeCSV(rs);
			long t2 = System.currentTimeMillis();
			System.out.println("time: " + (t2 - t1));
			times.add(t2 - t1);
			graph1.close();
		}
	}

	public UserRequest createUserRequest() {
		return new UserRequest();
	}

	private void writeCSV(ResultSet rs) {
		String outputFile = "resultlog/sparql/queryresults.csv";

		// before we open the file check to see if it already exists
		boolean alreadyExists = new File(outputFile).exists();

		try {
			// use FileWriter constructor that specifies open for appending
			CsvWriter csvOutput = new CsvWriter(new FileWriter(outputFile, true), ',');

			// if the file didn't already exist then we need to write out the header line
			if (!alreadyExists) {
				csvOutput.write("serviceID");
				csvOutput.write("parameterType");
				csvOutput.write("parameterValue");
				csvOutput.endRecord();
				for (; rs.hasNext();) {
					QuerySolution qs = rs.next();
					String sid = qs.get("serviceId").toString();
					// if (sid.contains("SimService-"))
					// continue;
					csvOutput.write(sid);
					csvOutput.write(qs.get("type").toString());
					csvOutput.write(qs.get("value").toString());
					csvOutput.endRecord();
				}
			}

			csvOutput.close();
		} catch (IOException e) {
			e.printStackTrace();
		}
	}

	private void queryDefaultGraph() {
		VirtGraph graph1 = new VirtGraph("iot.ee.surrey.ac.uk/citypulse/datasets/servicerepository", sefkiVirtuosuoURL,
				"dba", "iotest");
		Query sparql = QueryFactory.create("prefix rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#> "
				+ "prefix emvo: <http://sense.deri.ie/vocab/emvo#> "
				+ "prefix owlssp: <http://www.daml.org/services/owl-s/1.2/ServiceParameter.owl#> "
				+ "prefix owls: <http://www.daml.org/services/owl-s/1.2/Service.owl#> "
				+ "prefix rdfs: <http://www.w3.org/2000/01/rdf-schema#> "
				+ "prefix ces: <http://www.insight-centre.org/ces#> " + "select ?serviceId ?type ?value where { "
				+ "?serviceId owls:presents ?profile.  ?profile ?hasnfp ?x.  ?x rdf:type ?type.  "
				+ "?x owlssp:sParameter ?y.  ?y emvo:hasQuantityValue ?value }");
		VirtuosoQueryExecution vqe = VirtuosoQueryExecutionFactory.create(sparql, graph1);
		long t1 = System.currentTimeMillis();
		ResultSet rs = vqe.execSelect();
		long t3 = System.currentTimeMillis();
		int sum = 0;
		for (; rs.hasNext();) {
			rs.next();
			sum++;
		}
		long t2 = System.currentTimeMillis();
		System.out.println("time: " + (t2 - t1) + ", " + (t3 - t1) + ", results: " + sum);
		graph1.close();
	}

	private static void queryDefaultGraphViaSparqlEndpoint() {
		String query = "select * where {?s ?p ?o} limit 10000000";
		QueryExecution qe = QueryExecutionFactory.sparqlService(sefkiVirtuosuoURL, query);
		ResultSet rs = qe.execSelect();
		long sum = 0;
		for (; rs.hasNext();) {
			QuerySolution qs = rs.next();
			// System.out.println(qs.get("cnt"));
			// System.out.println(qs.get("s") + " " + qs.get("p") + " " + qs.get("o"));
			sum += 1;
			System.out.println(sum);
		}
		// System.out.println(rs.());
	}

	public static void main(String[] args) throws SQLException, InterruptedException {
		VirtuosoTest vdm = new VirtuosoTest();
		// VirtuosoTest.queryDefaultGraphViaSparqlEndpoint();
		try {
			// Class.forName("virtuoso.jdbc4.Driver");
			// Connection conn = DriverManager.getConnection(sefkiVirtuosuoURL, "dba", "iotest");
			VirtGraph graph1 = new VirtGraph(defaultGraphName, sefkiVirtuosuoURL, "dba", "iotest");
			Query sparql = QueryFactory.create("prefix rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#> "
					+ "prefix emvo: <http://sense.deri.ie/vocab/emvo#> "
					+ "prefix owlssp: <http://www.daml.org/services/owl-s/1.2/ServiceParameter.owl#> "
					+ "prefix owls: <http://www.daml.org/services/owl-s/1.2/Service.owl#> "
					+ "prefix rdfs: <http://www.w3.org/2000/01/rdf-schema#> "
					+ "prefix ces: <http://www.insight-centre.org/ces#> " + "select ?serviceId where { "
					+ "?serviceId owls:presents ?profile. }");
			VirtuosoQueryExecution vqe = VirtuosoQueryExecutionFactory.create(sparql, graph1);
			long t1 = System.currentTimeMillis();
			ResultSet rs = vqe.execSelect();
			// writeCSV(rs);
			long t2 = System.currentTimeMillis();
			System.out.println("time: " + (t2 - t1) + ", results: " + rs.getResultVars().size());
			times.add(t2 - t1);
			graph1.close();
			// java.sql.Statement st = conn.createStatement();
		} catch (Exception e) {
			e.printStackTrace();
		}
		// vdm.loadDataset();
		// int userCnt = 1;
		// for (int i = 0; i < userCnt; i++) {
		// UserRequest ur = vdm.createUserRequest();
		// new Thread(ur).start();
		// }
		// while (vdm.times.size() < userCnt)
		// Thread.sleep(500);
		// long sum = 0;
		// for (long l : vdm.times)
		// sum += l;
		// System.out.println("Avg. query time: " + (double) (sum / times.size()));
		// vdm.loadDataset();
		// vdm.queryDefaultGraph();
	}
}
