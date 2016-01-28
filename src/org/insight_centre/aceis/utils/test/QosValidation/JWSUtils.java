package org.insight_centre.aceis.utils.test.QosValidation;

import java.util.Date;

public class JWSUtils {
	@SuppressWarnings("deprecation")
	public static Date getTimeStampFromAggregatedData(String str) {
		String[] array = str.split("-");
		String year = array[0];
		String month = array[1];
		String day = array[2];
		String h = array[3];
		String m = array[4];
		String s = array[5];
		// System
		Date d = new Date();
		d.setYear(Integer.parseInt(year));
		d.setMonth(Integer.parseInt(month));
		d.setDate(Integer.parseInt(day));
		d.setHours(Integer.parseInt(h));
		d.setMinutes(Integer.parseInt(m));
		d.setSeconds(Integer.parseInt(s));
		return d;
	}
}
