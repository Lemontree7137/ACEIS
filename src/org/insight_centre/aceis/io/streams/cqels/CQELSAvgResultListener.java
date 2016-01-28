package org.insight_centre.aceis.io.streams.cqels;

import java.util.Iterator;

import org.deri.cqels.data.Mapping;
import org.deri.cqels.engine.ContinuousListener;
import org.insight_centre.aceis.engine.ACEISFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.hp.hpl.jena.sparql.core.Var;

public class CQELSAvgResultListener implements ContinuousListener {
	private static final Logger logger = LoggerFactory.getLogger(CQELSAvgResultListener.class);

	@Override
	public void update(Mapping mapping) {
		String result = "";
		try {
			for (Iterator<Var> vars = mapping.vars(); vars.hasNext();)
				result += " " + ACEISFactory.getACEISInstance().getContext().engine().decode(mapping.get(vars.next()));
		} catch (Exception e) {
			logger.error("CQELS decoding error: " + e.getMessage());
		}

	}

}
