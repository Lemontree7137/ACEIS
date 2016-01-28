package org.insight_centre.aceis.querytransformation;

import org.insight_centre.aceis.eventmodel.EventDeclaration;

import com.hp.hpl.jena.rdf.model.Model;

public interface QueryTransformer {
	public String transformFromRDF(Model m);

	public String getTimeWindowStr();

	public void setTimeWindowStr(String str);

	public TransformationResult transformFromED(EventDeclaration m) throws Exception;
}
