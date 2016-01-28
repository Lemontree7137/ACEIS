package org.insight_centre.aceis.utils.test.qos;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileWriter;
import java.io.IOException;
import java.text.DecimalFormat;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Date;
import java.util.HashMap;
import java.util.Map;
import java.util.Map.Entry;
import java.util.UUID;

import org.insight_centre.aceis.eventmodel.QosVector;
import org.insight_centre.aceis.io.rdf.RDFFileManager;
import org.insight_centre.aceis.observations.QosUpdate;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.csvreader.CsvReader;
import com.csvreader.CsvWriter;
import com.hp.hpl.jena.query.QueryExecution;
import com.hp.hpl.jena.query.QueryExecutionFactory;
import com.hp.hpl.jena.query.QuerySolution;
import com.hp.hpl.jena.query.ResultSet;
import com.hp.hpl.jena.rdf.model.Literal;
import com.hp.hpl.jena.rdf.model.Model;
import com.hp.hpl.jena.util.FileManager;

public class StreamQualityAnalyzer {

	public static Map<String, String> idMap = new HashMap<String, String>();
	static {
		try {
			CsvReader metaData;
			metaData = new CsvReader("dataset/MetaData/trafficMetaData.csv");
			metaData.readHeaders();
			while (metaData.readRecord()) {
				String id = metaData.get("extID");
				String rid = metaData.get("REPORT_ID");
				idMap.put(id, rid);
				// stream(n(RDFFileManager.defaultPrefix + streamData.get("REPORT_ID")),
				// n(RDFFileManager.ctPrefix + "hasETA"), n(data.getEstimatedTime() + ""));
				// System.out.println("metadata: " + metaData.toString());

			}
			metaData.close();
		} catch (FileNotFoundException e) {
			e.printStackTrace();
		} catch (IOException e) {
			e.printStackTrace();
		}

	}
	private final DecimalFormat df = new DecimalFormat("0.00");
	private final Logger logger = LoggerFactory.getLogger(this.getClass());

	private final SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss");

	public static void main(String[] args) throws IOException, ParseException {
		StreamQualityAnalyzer sqa = new StreamQualityAnalyzer();
		Map<String, ArrayList<QosUpdate>> qMap = sqa.loadQualityModel();
		for (Entry<String, ArrayList<QosUpdate>> e : qMap.entrySet()) {
			sqa.writeQosStream("qos-" + e.getKey() + ".stream", e.getValue());
			int size = e.getValue().size();
			System.out.println(e.getKey() + " size: " + size);
			for (int i = 0; i < 10; i++) {
				int offset = (int) (Math.random() * size);
				System.out.println(e.getKey() + " offsetting: " + offset);
				ArrayList<QosUpdate> offsetted = sqa.applyOffset(i, offset, e.getValue());
				sqa.writeQosStream("qos-" + e.getKey() + "-sim" + i + "-offset" + offset + ".stream", offsetted);
			}
		}
	}

	private ArrayList<QosUpdate> applyOffset(int cnt, int offset, ArrayList<QosUpdate> updates) {
		ArrayList<QosUpdate> results = new ArrayList<QosUpdate>();
		for (int i = offset; i < updates.size() + offset; i++) {
			QosUpdate currentUpdate = new QosUpdate(new Date(), "qu-" + UUID.randomUUID(), updates.get(0)
					.getCorrespondingServiceId() + "-sim" + cnt, new QosVector());
			Date currentDate = updates.get(i - offset).getObTimestamp();
			if (i < updates.size())
				currentUpdate.setQos(updates.get(i).getQos());
			else
				currentUpdate.setQos(updates.get(i - updates.size()).getQos());
			currentUpdate.setObTimestamp(currentDate);
			results.add(currentUpdate);
		}
		Collections.sort(results);
		return results;

	}

	private void writeQosStream(String fileName, ArrayList<QosUpdate> updates) {
		String outputFile = "streams/qos/" + fileName;

		// before we open the file check to see if it already exists
		boolean alreadyExists = new File(outputFile).exists();

		try {
			// use FileWriter constructor that specifies open for appending
			CsvWriter csvOutput = new CsvWriter(new FileWriter(outputFile, true), ',');

			// if the file didn't already exist then we need to write out the header line
			if (!alreadyExists) {
				csvOutput.write("id");
				csvOutput.write("sensorId");
				csvOutput.write("timestamp");
				csvOutput.write("accuracy");
				csvOutput.write("latency");
				csvOutput.write("completeness");
				csvOutput.write("security");
				csvOutput.write("traffic");
				csvOutput.write("price");
				csvOutput.endRecord();
			}
			for (QosUpdate qu : updates) {
				csvOutput.write(qu.getId());
				csvOutput.write(qu.getCorrespondingServiceId());
				csvOutput.write(sdf.format(qu.getObTimestamp()));
				csvOutput.write(qu.getQos().getAccuracy() + "");
				csvOutput.write(qu.getQos().getLatency() + "");
				csvOutput.write(qu.getQos().getReliability() + "");
				csvOutput.write(qu.getQos().getSecurity() + "");
				csvOutput.write(qu.getQos().getTraffic() + "");
				csvOutput.write(qu.getQos().getPrice() + "");
				csvOutput.endRecord();
			}

			csvOutput.close();
		} catch (IOException e) {
			e.printStackTrace();
		}
	}

	private String getFileName(String getnodeId) throws IOException {
		return idMap.get(getnodeId.split("#")[1]);
	}

	private Map<String, ArrayList<QosUpdate>> loadQualityModel() throws IOException, ParseException {
		Map<String, ArrayList<QosUpdate>> results = new HashMap<String, ArrayList<QosUpdate>>();
		int cnt = 0;
		for (String sensorId : idMap.values()) {
			// String sensorId = this.getFileName(ed.getnodeId());
			results.put(sensorId, new ArrayList<QosUpdate>());
			Model m = FileManager.get().loadModel(
					"../Usecase_Sceanrio_1_Implementation/Quality/scenario1-traffic-rated/scenario1-" + sensorId
							+ ".ttl");
			System.out.println(m);
			String queryStr = RDFFileManager.queryPrefix
					+ " select  ?time ?correctnessV ?latencyV where "
					+ "{?sample a sao:Segment. ?sample ssn:observationResultTime ?x. ?x tl:at ?time. "
					+ "?sample qoi:hasQuality ?correctness. ?correctness a qoi:Correctness. ?correctness qoi:value ?correctnessV. "
					+ "?sample qoi:hasQuality ?latency. ?latency a qoi:Latency. ?latency qoi:value ?latencyV." + " }";
			QueryExecution qe = QueryExecutionFactory.create(queryStr, m);
			ResultSet queryResults = qe.execSelect();
			String type = "";
			while (queryResults.hasNext()) {
				QuerySolution qs = queryResults.next();
				Literal time = qs.getLiteral("time");
				Literal correctness = qs.getLiteral("correctnessV");
				Literal latency = qs.getLiteral("latencyV");
				Date timesValue = this.sdf.parse(time.getString());
				Double correctnessV = correctness.getDouble();
				Double latencyV = latency.getDouble();
				QosVector qos = new QosVector();
				qos.setAccuracy(correctnessV);
				// qos.setLatency(latency);
				// qos.setReliability(c);
				QosUpdate qu = new QosUpdate(timesValue, "qu-" + UUID.randomUUID(), sensorId, qos);
				results.get(sensorId).add(qu);
			}
			Collections.sort(results.get(sensorId));
			cnt++;
			// if (cnt > 3)
			// break;
		}

		return results;
	}

}
