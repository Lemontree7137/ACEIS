package org.insight_centre.aceis.utils.test.QosValidation;

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.util.Map.Entry;
import java.util.TreeMap;

import com.csvreader.CsvWriter;

public class TOITQosValidationMonitor implements Runnable {

	@Override
	public void run() {
		int cnt = 0;
		while (!ToitQosValidationTest.anyStreamStopped()) {
			try {
				Thread.sleep(5000);
			} catch (InterruptedException e) {

				e.printStackTrace();
			}
			cnt += 1;
			Double delay = ToitQosValidationTest.getAvgDelay();
			// if (delay.equals(Double.NaN))
			ToitQosValidationTest.latencyOverTime.put(cnt * 5, delay);
			// else
			// JWSTest.latencyOverTime.put(cnt * 5, 0.0);
		}
		this.printResults();
	}

	private void printResults() {
		// System.out.println("Printing Result...");
		TreeMap<Integer, Double> sortedLatency = new TreeMap(ToitQosValidationTest.latencyOverTime);
		long inputCnt = 0;
		for (Long i : ToitQosValidationTest.inBytesMap.values()) {
			inputCnt += i;
		}
		System.out.println("Total inputs: " + inputCnt);
		System.out.println("Total outputs: " + ToitQosValidationTest.outByteCnt);
		System.out.println("Delay over time series: " + sortedLatency);
		writeCSV(inputCnt, ToitQosValidationTest.outByteCnt, sortedLatency);
		System.exit(0);
	}

	private void writeCSV(Long input, Long output, TreeMap<Integer, Double> sortedLatency) {
		String outputFile = "resultLog/jws/" + ToitQosValidationTest.mode + ".csv";

		// before we open the file check to see if it already exists
		boolean alreadyExists = new File(outputFile).exists();

		try {
			// use FileWriter constructor that specifies open for appending
			CsvWriter csvOutput = new CsvWriter(new FileWriter(outputFile, true), ',');

			// if the file didn't already exist then we need to write out the header line
			if (!alreadyExists) {
				// csvOutput.write("input");
				// csvOutput.write("output");
				// // csvOutput.write("timestamp");
				// // csvOutput.write("accuracy");
				// // csvOutput.write("latency");
				// // csvOutput.write("completeness");
				// // csvOutput.write("security");
				// // csvOutput.write("traffic");
				// // csvOutput.write("price");
				// csvOutput.endRecord();
				// csvOutput.write(input + "");
				// csvOutput.write(output + "");
				// csvOutput.endRecord();
				// }
				String latest = "";
				for (Entry e : sortedLatency.entrySet()) {
					csvOutput.write(e.getKey() + "");
					if (!e.getValue().toString().equals("NaN")) {
						csvOutput.write(e.getValue() + "");
						latest = e.getValue() + "";
					} else
						csvOutput.write(latest);
					// csvOutput.write(qu.getCorrespondingServiceId());
					// csvOutput.write(sdf.format(qu.getObTimestamp()));
					// csvOutput.write(qu.getQos().getAccuracy() + "");
					// csvOutput.write(qu.getQos().getLatency() + "");
					// csvOutput.write(qu.getQos().getReliability() + "");
					// csvOutput.write(qu.getQos().getSecurity() + "");
					// csvOutput.write(qu.getQos().getTraffic() + "");
					// csvOutput.write(qu.getQos().getPrice() + "");
					csvOutput.endRecord();
				}
			}

			csvOutput.close();
		} catch (IOException e) {
			e.printStackTrace();
		}
	}
}
