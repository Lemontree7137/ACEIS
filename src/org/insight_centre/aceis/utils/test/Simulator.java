package org.insight_centre.aceis.utils.test;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

//import org.apache.poi.openxml4j.exceptions.InvalidFormatException;
////import org.apache.poi.openxml4j.exceptions.InvalidFormatException;
//import org.apache.poi.ss.usermodel.Cell;
//import org.apache.poi.ss.usermodel.CellStyle;
//import org.apache.poi.ss.usermodel.Row;
//import org.apache.poi.ss.usermodel.Sheet;
//import org.apache.poi.ss.usermodel.Workbook;
//import org.apache.poi.ss.usermodel.WorkbookFactory;
import org.insight_centre.aceis.engine.ReusabilityHierarchy;
import org.insight_centre.aceis.eventmodel.EventDeclaration;
import org.insight_centre.aceis.eventmodel.EventOperator;
import org.insight_centre.aceis.eventmodel.EventPattern;
import org.insight_centre.aceis.eventmodel.OperatorType;

/**
 * @author feng
 * 
 *         Obsolete test class
 * 
 */
public class Simulator {
	private static HashMap<Integer, String> dataCollect() throws IOException {
		File file = new File("patterns/reduction.txt");
		FileReader fr = new FileReader(file);
		BufferedReader br = new BufferedReader(fr);
		String resultStr = br.readLine();
		HashMap<Integer, String> results = new HashMap<Integer, String>();
		while (resultStr != null) {
			// resultStr = resultStr.trim().substring(0, resultStr.length() - 1);
			results.put(Integer.parseInt(resultStr.split("=")[0]), resultStr.split("=")[1]);
			// System.out.println("added: " + result.toString());
			resultStr = br.readLine();
		}
		br.close();
		System.out.println(results);
		return results;
	}

	private static void hierarchyTest() throws Exception {
		HashMap<String, Long> results = new HashMap<String, Long>();
		// for (int i = 1; i < 16; i++) {
		// int size = i * 100;
		// Simulator sim = new Simulator();
		// List<EventPattern> candidates = sim.createEPs(size, 3, 3);
		// for (EventPattern ep : candidates)
		// ep = ep.getReducedPattern();
		// Composer com = new Composer(null, candidates);
		// long start = System.currentTimeMillis();
		// com.buildHierarchy();
		// long end = System.currentTimeMillis();
		// long unitTime = end - start;
		// results.put("3-" + size, unitTime);
		//
		// }

		// for (int i = 1; i < 16; i++) {
		// int size = i * 100;
		// Simulator sim = new Simulator();
		// List<EventPattern> candidates = sim.createEPs(size, 4, 4);
		// for (EventPattern ep : candidates)
		// ep = ep.getReducedPattern();
		// Composer com = new Composer(null, candidates);
		// long start = System.currentTimeMillis();
		// com.buildHierarchy();
		// long end = System.currentTimeMillis();
		// long unitTime = start - end;
		// results.put("4-" + size, unitTime);
		//
		// }
		List<EventPattern> candidates = new ArrayList<EventPattern>();
		Simulator sim = new Simulator();
		for (int i = 1; i < 16; i++) {
			// int size = i * 100;

			candidates.addAll(sim.createEPs(100, 5, 5));
			// for (EventPattern ep : candidates)
			// ep = ep.getReducedPattern();
			ReusabilityHierarchy com = new ReusabilityHierarchy(null, candidates);
			long start = System.currentTimeMillis();
			com.buildHierarchy();
			long end = System.currentTimeMillis();
			long unitTime = end - start;
			results.put("5-" + candidates.size(), unitTime);

		}
		for (Map.Entry en : results.entrySet())
			System.out.println(en);
	}

	@SuppressWarnings("unused")
	public static void main(String[] args) throws Exception {
		// HashMap<Integer, String> results = Simulator.dataCollect();
		// Simulator.parseResults(results);
		// Simulator.reductionTest2(5, 5);
		// Simulator.hierarchyTest();
		// Simulator.compositionTest();
		// Simulator sim = new Simulator();
		// List<EventPattern> ep = sim.createEPs(500, 4, 3);
		// int cnt = 0;
		// TextFileManager.writePatternToFile("pat.txt", new Simulator().createEPs(300, 3, 4));
		// for (EventPattern e : ep)
		// cnt += e.getSize();
		// System.out.println(cnt / 500);
		// // Reduction test
		writeLocationFile("UserLocationService.stream");
	}

	private static void writeLocationFile(String fileName) throws IOException {
		File newFile = new File("streams/" + fileName);
		FileWriter fw = new FileWriter(newFile);
		BufferedWriter bw = new BufferedWriter(fw);
		for (int i = 0; i < 1000; i++) {
			bw.write("http://www.ict-citypulse.eu/ontologies/userprofile#user-007|" + (56 + Math.random()) + ","
					+ (10 + Math.random()));
			bw.newLine();
		}
		bw.close();
	}

	// private static void parseResults(HashMap<Integer, String> results) throws InvalidFormatException,
	// FileNotFoundException, IOException {
	// String inputFileName = "patterns/reduction.xls";
	// // POIFSFileSystem poifs = new POIFSFileSystem(inp);
	// File inputFile = new File(inputFileName);
	// Workbook wb = WorkbookFactory.create(new FileInputStream(inputFile));
	// // Workbook wb =
	// // WorkbookFactory.create(poifs.createDocumentInputStream("resources/workbook.xls"));
	// FileOutputStream out = new FileOutputStream("patterns/reduction.xls");
	// Sheet s = wb.getSheetAt(0);
	// CellStyle cs = wb.createCellStyle();
	// cs.setWrapText(true);
	// // s.set
	// Row row = s.createRow((short) 0);
	// Cell sizeHead = row.createCell(0);
	// sizeHead.setCellValue("size");
	// Cell minHead = row.createCell(1);
	// minHead.setCellValue("min");
	// Cell maxHead = row.createCell(2);
	// maxHead.setCellValue("max");
	// Cell avgHead = row.createCell(3);
	// avgHead.setCellValue("avg");
	// int sizeCnt = 20;
	// for (int i = 1; i < 200; i++) {
	// Row dataRow = s.createRow(i);
	// Cell sizeCell = dataRow.createCell(0);
	// sizeCell.setCellStyle(cs);
	// if (results.get(sizeCnt) != null) {
	// sizeCell.setCellValue(sizeCnt);
	// Double count = Double.parseDouble(results.get(sizeCnt).split("-")[0].split(",")[0]);
	// if (count >= 10.0) {
	// Double avg = Double.parseDouble(results.get(sizeCnt).split("-")[0].split(",")[1]);
	// int max = Integer.parseInt(results.get(sizeCnt).split("-")[1]);
	// int min = Integer.parseInt(results.get(sizeCnt).split("-")[2]);
	// Cell minCell = dataRow.createCell(1);
	// minCell.setCellStyle(cs);
	// minCell.setCellValue(min);
	// Cell maxCell = dataRow.createCell(2);
	// maxCell.setCellStyle(cs);
	// maxCell.setCellValue(max);
	// Cell avgCell = dataRow.createCell(3);
	// avgCell.setCellStyle(cs);
	// avgCell.setCellValue(avg);
	// }
	// }
	// sizeCnt += 1;
	// }
	// wb.write(out);
	// out.close();
	// }

	private static void reductionTest(int degree, int height) throws Exception {
		Simulator sim = new Simulator();
		HashMap<Integer, String> results = new HashMap<Integer, String>();
		// for (int i = 0; i < 11; i++) {
		// int epSize = i * 100;
		List<EventPattern> list = sim.createEPs(5000, degree, height);

		// int totalSize = 0;
		// long totalTime = 0;
		// long leastTime = -1;
		// long maxTime = -1;
		for (EventPattern ep : list) {
			long currentTime = System.currentTimeMillis();

			ep.getReducedPattern();
			long endTime = System.currentTimeMillis();
			long unitTime = endTime - currentTime;
			if (results.get(ep.getSize()) == null)
				results.put(ep.getSize(), "1," + unitTime + "-" + unitTime + "-" + unitTime);
			else {
				double totalNumber = Double.parseDouble(results.get(ep.getSize()).split("-")[0].split(",")[0]);
				// totalNumber += 1;

				double avgTime = Double.parseDouble(results.get(ep.getSize()).split("-")[0].split(",")[1]);
				avgTime = ((totalNumber * avgTime) + (double) unitTime) / (totalNumber + 1.0);

				long maxTime = Long.parseLong(results.get(ep.getSize()).split("-")[1]);
				if (maxTime <= unitTime)
					maxTime = unitTime;
				long minTime = Long.parseLong(results.get(ep.getSize()).split("-")[2]);
				if (minTime >= unitTime)
					minTime = unitTime;
				results.put(ep.getSize(), (totalNumber + 1) + "," + avgTime + "-" + maxTime + "-" + minTime);
			}
		}

		// System.out.println("size: " + totalSize);
		// System.out.println("time: " + (endTime - currentTime));
		// }
		System.out.println(results);
	}

	private static void reductionTest2(int degree, int height) throws Exception {
		Simulator sim = new Simulator();
		HashMap<Integer, List<EventPattern>> results = new HashMap<Integer, List<EventPattern>>();
		// for (int i = 0; i < 11; i++) {
		// int epSize = i * 100;
		for (int i = 1; i < 11; i++)
			results.put(100 * i, new ArrayList<EventPattern>());
		results.put(9000, new ArrayList<EventPattern>());
		List<EventPattern> list = sim.createEPs(5000, degree, height);

		// int totalSize = 0;
		// long totalTime = 0;
		// long leastTime = -1;
		// long maxTime = -1;
		for (EventPattern ep : list) {
			long currentTime = System.currentTimeMillis();
			ep.getReducedPattern();
			long endTime = System.currentTimeMillis();
			long unitTime = endTime - currentTime;
			if (unitTime <= 100)
				results.get(100).add(ep);
			else if (unitTime <= 200)
				results.get(200).add(ep);
			else if (unitTime <= 300)
				results.get(300).add(ep);
			else if (unitTime <= 400)
				results.get(400).add(ep);
			else if (unitTime <= 500)
				results.get(500).add(ep);
			else if (unitTime <= 600)
				results.get(600).add(ep);
			else if (unitTime <= 700)
				results.get(700).add(ep);
			else if (unitTime <= 800)
				results.get(800).add(ep);
			else if (unitTime <= 900)
				results.get(900).add(ep);
			else if (unitTime <= 1000)
				results.get(1000).add(ep);
			else
				results.get(9000).add(ep);
		}
		for (Map.Entry en : results.entrySet())
			System.out.println(en.getKey() + " = " + ((List) en.getValue()).size());
	}

	private List<EventPattern> eps;

	private int idcnt = 0;

	// private ArrayList<EventDeclaration> createPrimitiveEvents(int n) {
	// ArrayList<EventDeclaration> results = new ArrayList<EventDeclaration>();
	// for (int m = 0; m < n; m++) {
	// results.add(this.createRandomPrimitiveEventDeclaration());
	// }
	// return results;
	// }
	//
	// private EventDeclaration pickPrimitiveEvent(List<EventDeclaration> candidates) {
	// int index = (int) Math.random() * (candidates.size() - 1);
	// return candidates.get(index);
	// }

	// private int size,degree,height;
	public Simulator() {
		super();
		this.eps = new ArrayList<EventPattern>();
	}

	public Simulator(int size, int degree, int height) throws Exception {
		super();
		this.eps = createEPs(size, degree, height);
	}

	private List<EventPattern> createEPs(int size, int degree, int height) throws Exception {
		ArrayList<EventPattern> eps = new ArrayList<EventPattern>();
		for (int i = 0; i < size; i++) {
			eps.add(this.createRandomEventPattern(degree, height));
		}
		return eps;
	}

	private List<EventPattern> createEPs(int size, int degree, int height, Double reusePossibility) throws Exception {
		// 0<= possibility <1
		ArrayList<EventPattern> eps = new ArrayList<EventPattern>();
		for (int i = 0; i < size; i++) {
			Double rand = Math.random();
			if (rand > reusePossibility || eps.size() == 0)
				eps.add(this.createRandomEventPattern(degree, height).getReducedPattern());
			else {
				EventPattern template = eps.get((int) (Math.random() * eps.size())).clone();
				EventPattern newPattern = new EventPattern(template.getID(), new ArrayList<EventDeclaration>(),
						new ArrayList<EventOperator>(), new HashMap<String, List<String>>(),
						new HashMap<String, String>(), template.getTimeWindow(), 1);
				newPattern.setID(template.getID() + "clone" + this.idcnt);
				this.idcnt += 1;
				EventOperator newRoot = new EventOperator(OperatorType.or, 1, template.getIdCnt() + "");
				newPattern.getEos().add(newRoot);
				newPattern.getProvenanceMap().put(newRoot.getID(), new ArrayList<String>());
				newPattern.setIdCnt(template.getIdCnt() + 1);
				List<EventPattern> dsts = new ArrayList<EventPattern>();
				dsts.add(template);
				newPattern.addDSTs(dsts, newRoot);
				// EventOperator root=(EventOperator) template.getNodeById(template.)
				// if (root.getOpt() != OperatorType.rep) {
				EventDeclaration ed = this.createRandomPrimitiveEventDeclaration(newPattern);
				newPattern.getEds().add(ed);
				newPattern.setIdCnt(newPattern.getIdCnt() + 1);
				newPattern.getProvenanceMap().get(newRoot.getID()).add(ed.getnodeId());
				// newPattern.appendDST(ed);
				// } else {
				// root.setCardinality(root.getCardinality() * 2);
				// }
				eps.add(newPattern.getReducedPattern());
			}

		}
		return eps;
	}

	private EventPattern createRandomEventPattern(int degree, int height) {// creates random complete pattern/tree
		EventPattern ep = new EventPattern("EP" + this.idcnt, new ArrayList<EventDeclaration>(),
				new ArrayList<EventOperator>(), new HashMap<String, List<String>>(), new HashMap<String, String>(), 1,
				0);
		this.idcnt += 1;
		EventOperator root = this.createRandomOperator(ep);
		ep.setIdCnt(ep.getIdCnt() + 1);
		ep.getEos().add(root);
		this.expand(ep, root.getID(), 0, height, degree);
		// ep.setIdCnt(ep.getSize());
		// ep.print(0);
		return ep;
	}

	private EventOperator createRandomOperator(EventPattern ep) {
		Double typeIndicator = Math.random();
		OperatorType type;
		int cardinality = 1;
		if (typeIndicator < 0.25)// determine sensor type
			type = OperatorType.and;
		else if (typeIndicator < 0.5)
			type = OperatorType.or;
		else if (typeIndicator < 0.75)
			type = OperatorType.seq;
		else {
			type = OperatorType.rep;
			cardinality = (int) (Math.random() * 6 + 2);
		}
		String id = "" + ep.getIdCnt();
		EventOperator result = new EventOperator(type, cardinality, id);
		// this.idcnt += 1;
		// result.print();
		return result;
	}

	private EventDeclaration createRandomPrimitiveEventDeclaration(EventPattern ep) {
		Double typeIndicator = Math.random();
		// Double freq = Math.random() * 5 + 0.1;
		String type = "";
		if (typeIndicator < 0.2)// determine sensor type
			type = "temperature" + (int) (Math.random() * 2);
		else if (typeIndicator < 0.4)
			type = "humidity" + (int) (Math.random() * 2);
		else if (typeIndicator < 0.6)
			type = "motion" + (int) (Math.random() * 2);
		else if (typeIndicator < 0.8)
			type = "windspeed" + (int) (Math.random() * 2);
		else
			type = "light" + (int) (Math.random() * 2);
		String id = "" + ep.getIdCnt();
		// this.idcnt += 1;
		String src = "exampleuri:" + type;
		// if(freqIndicator<0.2)
		EventDeclaration result = new EventDeclaration(id, src, type, null, null, Double.parseDouble((type.charAt(type
				.length() - 1) + "")) + 1);
		// result.print();
		return result;
	}

	private void expand(EventPattern ep, String nodeId, int depthCnt, int height, int degree) {
		if (depthCnt < height - 1) {// can have operators or primitive events as children
			int childNum = (int) (Math.random() * (degree - 1) + 2);
			ep.getProvenanceMap().put(nodeId, new ArrayList<String>());
			for (int i = 0; i < childNum; i++) {
				Double random = Math.random();
				if (random < 0.5) {
					EventDeclaration ed = this.createRandomPrimitiveEventDeclaration(ep);
					ep.getEds().add(ed);
					ep.setIdCnt(ep.getIdCnt() + 1);
					ep.getProvenanceMap().get(nodeId).add(ed.getnodeId());
				} else {
					EventOperator eo = this.createRandomOperator(ep);
					ep.getEos().add(eo);
					ep.setIdCnt(ep.getIdCnt() + 1);
					ep.getProvenanceMap().get(nodeId).add(eo.getID());
					this.expand(ep, eo.getID(), depthCnt + 1, height, degree);
				}
			}

		}
		if (depthCnt == height - 1) {// creates only primitive events as children
			int childNum = (int) (Math.random() * (degree - 1) + 2);
			ep.getProvenanceMap().put(nodeId, new ArrayList<String>());
			for (int i = 0; i < childNum; i++) {
				EventDeclaration ed = this.createRandomPrimitiveEventDeclaration(ep);
				ep.getEds().add(ed);
				ep.setIdCnt(ep.getIdCnt() + 1);
				ep.getProvenanceMap().get(nodeId).add(ed.getnodeId());
			}
		}
		EventOperator currentNode = (EventOperator) ep.getNodeById(nodeId);// add temporal relations
		if (currentNode.getOpt() == OperatorType.seq || currentNode.getOpt() == OperatorType.rep) {
			ArrayList<String> sequenceNodes = (ArrayList<String>) ep.getProvenanceMap().get(nodeId);
			for (int i = 0; i < sequenceNodes.size() - 1; i++) {
				ep.getTemporalMap().put(sequenceNodes.get(i), sequenceNodes.get(i + 1));
			}
		}
	}

	public List<EventPattern> getEps() {
		return eps;
	}

	private EventPattern makeSequencePattern(String seq) {
		EventPattern ep = new EventPattern("EP0", new ArrayList<EventDeclaration>(), new ArrayList<EventOperator>(),
				new HashMap<String, List<String>>(), new HashMap<String, String>(), 1, 0);
		EventOperator root = new EventOperator(OperatorType.seq, 1, "0");
		ep.getEos().add(root);
		ep.setIdCnt(seq.length() + 1);
		ep.getProvenanceMap().put(root.getID(), new ArrayList<String>());
		for (int i = 0; i < seq.length(); i++) {
			String edid = ep.getSize() + "";
			EventDeclaration ed = new EventDeclaration(ep.getSize() + "", "", seq.charAt(i) + "", null, null, 1.0);
			ep.getEds().add(ed);
			ep.getProvenanceMap().get(root.getID()).add(edid);
			if (i > 0) {
				String previousId = (ep.getSize() - 2) + "";
				ep.getTemporalMap().put(previousId, edid);
			}
		}
		return ep;
	}

	// private static void compositionTest() throws Exception {
	// Simulator sim = new Simulator();
	// List<EventPattern> list = sim.createEPs(1000, 3, 4, 0.2);
	// List<EventPattern> queries = TextFileManager.readPatternFromFile("patterns/pat.txt");
	// Composer com = new Composer(null, list);
	// com.buildHierarchy();
	// long currentTime = System.currentTimeMillis();
	// for (int i = 0; i < 4; i++)
	// // execute same query 5 times
	// for (EventPattern ep : queries)
	// com.getCompositionByReuse(ep);
	// long endTime = System.currentTimeMillis();
	// System.out.println("t1:" + (endTime - currentTime) / 12);
	// long currentTime2 = System.currentTimeMillis();
	// for (int i = 0; i < 4; i++)
	// for (EventPattern query : queries) {
	// com.getCompositionBySubstitution(query);
	// }
	// long endTime2 = System.currentTimeMillis();
	// System.out.println("t2:" + (endTime2 - currentTime2) / 12);
	// }

	public void setEps(List<EventPattern> eps) {
		this.eps = eps;
	}
}
