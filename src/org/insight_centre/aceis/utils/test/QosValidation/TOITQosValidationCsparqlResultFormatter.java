package org.insight_centre.aceis.utils.test.QosValidation;

import java.util.Date;
import java.util.HashSet;
import java.util.Set;

import org.insight_centre.aceis.observations.SensorObservation;

import eu.larkc.csparql.common.RDFTable;
import eu.larkc.csparql.common.RDFTuple;
import eu.larkc.csparql.common.streams.format.GenericObservable;
import eu.larkc.csparql.core.ResultFormatter;

public class TOITQosValidationCsparqlResultFormatter extends ResultFormatter {

	private Set<String> latestResults = new HashSet<String>();
	int cntSum = 0;
	int updates = 0;
	int min = 1000;
	int max = 0;

	@Override
	public void update(GenericObservable<RDFTable> observed, RDFTable q) {
		int validupdates = 0;
		for (final RDFTuple t : q) {
			System.out.println(t.toString());
			String result = t.toString().replaceAll("\t", " ").trim();
			if (!this.latestResults.contains(result)) {
				this.latestResults.add(result);
				ToitQosValidationTest.outByteCnt += result.getBytes().length;
				String[] results = result.split(" ");
				validupdates++;
				// JWSTest.latencyMap.clear();

				/*
				 * vehicle cnt
				 */
				// int cntSum = 0;
				// int thisSum = 0;
				// for (String s : results) {
				// double cnt = Double.parseDouble(s.substring(s.indexOf("\"") + 1, s.lastIndexOf("\"")));
				// thisSum += cnt;
				// }
				// if (min > thisSum)
				// min = thisSum;
				// if (max < thisSum)
				// max = thisSum;
				// cntSum += thisSum;
				// updates += 1;
				// System.out.println("avg vehicle on route: " + cntSum / updates + ", min: " + min + ", max: " + max);
				/*
				 * end of vehicle cnt
				 */

				/*
				 * jws latency test
				 */
				for (int i = 0; i < results.length; i += 2) {
					String obId = results[i];
					// System.out.println("getting ob id: " + obId);
					SensorObservation so = ToitQosValidationTest.obMap.get(obId);
					ToitQosValidationTest.latencyMap.put(so, System.currentTimeMillis());
				}
				/*
				 * end of jws latency test
				 */
				// System.out.println(results.length);
				// if (session != null)
				// sendResultToSession(results);
				// else
				// sendResultToEngine(results);
			}
		}
		System.out.println("Csparql results with " + validupdates + " valid updates @" + new Date());
	}
}
