package org.insight_centre.aceis.utils;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map.Entry;
import java.util.Set;
import java.util.UUID;

import org.insight_centre.aceis.engine.ACEISEngine;
import org.insight_centre.aceis.engine.CompositionPlanEnumerator;
import org.insight_centre.aceis.eventmodel.EventDeclaration;
import org.insight_centre.aceis.eventmodel.EventOperator;
import org.insight_centre.aceis.eventmodel.EventPattern;
import org.insight_centre.aceis.eventmodel.NodeRemovalException;
import org.insight_centre.aceis.eventmodel.OperatorType;
import org.insight_centre.aceis.eventmodel.QosVector;
import org.insight_centre.aceis.eventmodel.Selection;
import org.insight_centre.aceis.eventmodel.TrafficReportService;
import org.insight_centre.aceis.io.EventRepository;
import org.insight_centre.aceis.io.rdf.RDFFileManager;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ServiceGenerator {
	public static final Logger logger = LoggerFactory.getLogger(ServiceGenerator.class);
	public static ACEISEngine engine = null;

	public static List<EventPattern> createRandomAndQueries(ACEISEngine engine, int size, int cnt)
			throws CloneNotSupportedException, NodeRemovalException {
		List<EventPattern> results = new ArrayList<EventPattern>();

		for (int i = 0; i < cnt; i++) {
			int randSize;// = (int) (Math.random() * (size - 2) + 2);
			if (Math.random() < 0.5)
				randSize = 2;
			else
				randSize = 2;
			List<EventDeclaration> randEds = getRandomPrimitiveED(engine, randSize);
			List<String> eids = new ArrayList<String>();
			for (EventDeclaration ed : randEds) {
				// ed.setSrc(null);
				// ed.setMsgBusGrounding(null);
				// ed.setEid(null);
				// ed.setRid(null);
				// ed.setInternalQos(null);
				eids.add(ed.getServiceId());
			}

			results.add(createAndEvents(engine, eids, 1).get(0).getEp());
		}
		return results;
	}

	public static List<EventDeclaration> createAndEvents(ACEISEngine engine, List<String> originalEDs, int cnt)
			throws CloneNotSupportedException, NodeRemovalException {
		EventRepository er = engine.getRepo();
		List<EventDeclaration> results = new ArrayList<EventDeclaration>();
		List<List<EventDeclaration>> candidates = new ArrayList<List<EventDeclaration>>();
		CompositionPlanEnumerator cpe = new CompositionPlanEnumerator(er, null);

		for (String originalEdId : originalEDs) {
			List<EventDeclaration> partialCandidates = new ArrayList<EventDeclaration>();
			EventDeclaration oringialEd = cpe.getRepo().getEds().get(originalEdId);
			List<EventDeclaration> substitutes = cpe.getAllSubsituteEDforED(oringialEd);
			// System.out.println(substitutes.size() + " found for " + originalEdId);
			partialCandidates.addAll(substitutes);
			candidates.add(partialCandidates);
		}
		// System.out.println("candidates initialized.");
		List<String> codes = new ArrayList<String>();
		while (results.size() < cnt) {
			String code = "";
			if (results.size() == 0) {
				for (int i = 0; i < candidates.size(); i++) {
					int index = (int) (Math.random() * candidates.get(i).size());
					// if (Math.random() <= 0.5)
					code += "|" + index;
				}
			} else {
				int selected = 0;
				for (int i = 0; i < candidates.size(); i++) {
					int index = (int) (Math.random() * candidates.get(i).size());
					if (Math.random() <= 0.5) {
						code += "|" + index;
						selected += 1;
					} else
						code += "|-1";
				}
				if (selected < 2)
					continue;
			}
			code = code.substring(1, code.length());
			if (codes.contains(code))
				continue;
			else {
				// System.out.println("Adding code: " + code);
				List<String> subEventIds = new ArrayList<String>();
				List<EventDeclaration> subEvents = new ArrayList<EventDeclaration>();
				String[] codeArray = code.split("\\|");
				for (int i = 0; i < codeArray.length; i++) {
					if (!codeArray[i].contains("-")) {
						subEvents.add(candidates.get(i).get(Integer.parseInt(codeArray[i])).clone());
						subEventIds.add(subEvents.get(subEvents.size() - 1).getnodeId());
					}
				}
				int timeWindow = 3000;
				// for (String s : subEventIds)
				// subEvents.add(er.getEds().get(s));
				// System.out.println("subevents: " + subEvents.size());
				QosVector qos = QosVector.getRandomQos();
				EventDeclaration result = new EventDeclaration(RDFFileManager.defaultPrefix + "SimService-"
						+ UUID.randomUUID(), "", "complex", null, null, null, qos);
				EventPattern ep = new EventPattern();
				ep.setID("EP-" + result.getnodeId());
				ep.setEds(subEvents);
				String eoId = "EO-" + UUID.randomUUID();
				ep.getEos().add(new EventOperator(OperatorType.and, 1, eoId));
				ep.getProvenanceMap().put(eoId, subEventIds);
				for (EventDeclaration ed : subEvents) {
					String pName = "";
					String foi = "";
					String pType = "";
					for (String s : ed.getPayloads()) {
						if (s.split("\\|")[0].contains("AverageSpeed")) {
							pName = s.split("\\|")[2];
							foi = s.split("\\|")[1];
							if (!foi.contains("\\,")) {
								// logger.error("wrong foi str: " + foi + ", eventFoi: " + ed.getFoi());
								// System.exit(0);
								foi = ed.getFoi();
							}
							pType = s.split("\\|")[0];
						}
					}
					ep.getSelections().add(new Selection(pName, ed.getServiceId(), ed, foi, pType));
				}
				ep.setTimeWindow(timeWindow);
				result.setEp(ep);
				result.setSrc(result.getnodeId());
				result.setServiceId(result.getnodeId());
				results.add(result);
				// er.getEds().put(result.getnodeId(), result);
				// er.getEps().put(ep.getID(), ep);
			}
		}
		System.out.println("Sim eds created.");
		return results;
	}

	public static EventPattern createBaseEvent(int cnt) throws CloneNotSupportedException {
		EventPattern result = new EventPattern("EP-" + UUID.randomUUID(), new ArrayList<EventDeclaration>(),
				new ArrayList<EventOperator>(), new HashMap<String, List<String>>(), new HashMap<String, String>(), 1,
				0);
		EventOperator root = new EventOperator(OperatorType.or, 0, "EO-" + UUID.randomUUID());
		EventOperator and = new EventOperator(OperatorType.and, 0, "EO-" + UUID.randomUUID());
		EventOperator seq = new EventOperator(OperatorType.seq, 0, "EO-" + UUID.randomUUID());
		EventOperator rep = new EventOperator(OperatorType.rep, 2, "EO-" + UUID.randomUUID());

		result.getEos().add(root);
		result.getEos().add(and);
		result.getEos().add(seq);
		result.getEos().add(rep);

		ArrayList<String> idList = new ArrayList<String>();
		idList.add(and.getID());
		idList.add(seq.getID());
		idList.add(rep.getID());
		result.getProvenanceMap().put(root.getID(), idList);

		List<EventDeclaration> eds = getRandomPrimitiveED(engine, cnt);
		result.getEds().addAll(eds);

		ArrayList<String> idList2 = new ArrayList<String>();
		idList2.add(eds.get(0).getnodeId());
		idList2.add(eds.get(1).getnodeId());

		ArrayList<String> idList3 = new ArrayList<String>();
		idList3.add(eds.get(2).getnodeId());
		idList3.add(eds.get(3).getnodeId());
		result.getTemporalMap().put(eds.get(2).getnodeId(), eds.get(3).getnodeId());

		ArrayList<String> idList4 = new ArrayList<String>();
		idList4.add(eds.get(4).getnodeId());
		idList4.add(eds.get(5).getnodeId());
		result.getTemporalMap().put(eds.get(4).getnodeId(), eds.get(5).getnodeId());

		if (cnt > 6) {
			for (int i = 6; i < eds.size(); i++) {
				double rand = Math.random();
				EventDeclaration ed = eds.get(i);
				if (rand < 0.3333) {
					idList2.add(ed.getnodeId());
				} else if (rand < 0.666666666) {
					idList3.add(ed.getnodeId());
					result.getTemporalMap().put(idList3.get(idList3.size() - 2), idList3.get(idList3.size() - 1));
				} else {
					idList4.add(ed.getnodeId());
					result.getTemporalMap().put(idList4.get(idList4.size() - 2), idList4.get(idList4.size() - 1));
				}
			}
		}
		result.getProvenanceMap().put(and.getID(), idList2);
		result.getProvenanceMap().put(seq.getID(), idList3);
		result.getProvenanceMap().put(rep.getID(), idList4);
		for (EventDeclaration ed : eds) {
			// for (String s : ed.getPayloads())
			// logger.info("Payload: " + s);
			String s = ed.getPayloads().get(0);
			String pType = s.split("\\|")[0];
			String foi = s.split("\\|")[1];
			String pName = s.split("\\|")[2];
			Selection sel = new Selection(pName, ed.getnodeId(), ed, foi, pType);
			result.getSelections().add(sel);
		}
		// result.getProvenanceMap().put(root.getID(), and.getID());

		return result;
	}

	public static List<EventDeclaration> getRandomPrimitiveED(ACEISEngine engine, int cnt)
			throws CloneNotSupportedException {
		EventRepository er = engine.getRepo();
		Object[] edsInRepo = er.getEds().values().toArray();
		List<EventDeclaration> eds = new ArrayList<EventDeclaration>();
		Set<Integer> indexes = new HashSet<Integer>();
		while (eds.size() < cnt) {
			int index = (int) (Math.random() * er.getEds().size());
			if (indexes.contains(index) || !(edsInRepo[index] instanceof TrafficReportService))
				continue;
			indexes.add(index);
			EventDeclaration ed = new EventDeclaration("ED-" + UUID.randomUUID(), "", "", null,
					new ArrayList<String>(), 0.0);
			EventDeclaration originalEd = ((EventDeclaration) edsInRepo[index]).clone();
			ed.setEventType(originalEd.getEventType());
			ed.setFoi(originalEd.getFoi());
			ed.setInternalQos(originalEd.getInternalQos());
			ed.setFrequency(originalEd.getFrequency());
			ed.setServiceId(originalEd.getServiceId());
			ed.setSrc(originalEd.getSrc());
			ed.setPayloads(originalEd.getPayloads());
			eds.add(ed);
		}
		logger.info("eds retrieved: " + eds.size());
		return eds;
	}

	public static void main(String[] args) throws Exception {
		// for (int j = 5; j < 6; j++) {
		// ACEISEngine.initialize(Mode.CSPARQL, "jws/SimRepo-" + j + ".n3");
		engine.initialize("toitRepo/simrepo-5-10.n3");
		List<String> ids = new ArrayList<String>();
		ids.add("http://www.insight-centre.org/dataset/SampleEventService#230");
		ids.add("http://www.insight-centre.org/dataset/SampleEventService#228");
		ids.add("http://www.insight-centre.org/dataset/SampleEventService#226");
		// ids.add("http://www.insight-centre.org/dataset/SampleEventService#220");
		// ids.add("http://www.insight-centre.org/dataset/SampleEventService#218");
		// ids.add("http://www.insight-centre.org/dataset/SampleEventService#216");
		ids.add("http://www.insight-centre.org/dataset/SampleEventService#540");
		ids.add("http://www.insight-centre.org/dataset/SampleEventService#537");
		// ids.add("http://www.insight-centre.org/dataset/SampleEventService#322");
		for (int i = 100; i < 110; i += 10) {
			List<EventDeclaration> simEds = createAndEvents(engine, ids, i);
			engine.getRepo().getEps().clear();
			List<Entry<String, EventDeclaration>> edsToRemove = new ArrayList<Entry<String, EventDeclaration>>();
			for (Entry<String, EventDeclaration> en : engine.getRepo().getEds().entrySet()) {
				// en.getValue().setInternalQos(QosVector.getRandomQos());
				if (en.getValue().getEp() != null) {
					// System.out.println("removing ed: " + ed.getServiceId());
					edsToRemove.add(en);
				}
			}
			System.out.println("total eds to remove: " + edsToRemove.size());
			System.out.println("total eds b4: " + engine.getRepo().getEds().size());
			for (Entry s : edsToRemove) {
				engine.getRepo().getEds().entrySet().remove(s);
				// ACEISEngine.getRepo().getEps().entrySet().remove(((EventDeclaration)
				// s.getValue()).getEp().getID());
			}
			System.out.println("total eds after: " + engine.getRepo().getEds().size());
			for (EventDeclaration ed : simEds) {
				engine.getRepo().getEds().put(ed.getnodeId(), ed);
				engine.getRepo().getEps().put(ed.getEp().getID(), ed.getEp());
			}
			for (Entry<String, EventDeclaration> en : engine.getRepo().getEds().entrySet()) {
				en.getValue().setInternalQos(QosVector.getGoodRandomQos());
				en.getValue().setFrequency((Math.random() * 0.8) + 0.2);
			}
			System.out.println("Writing sim repository.");
			RDFFileManager.writeRepoToFile("toitRepo/simrepo-5-" + i + ".n3", engine.getRepo());
		}
	}
	// }
}
