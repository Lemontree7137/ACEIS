package org.insight_centre.aceis.eventmodel;

import org.insight_centre.aceis.engine.ACEISEngine.RspEngine;

public class EventRequest {
	private EventPattern ep;
	private QosVector constraint;
	private AggregateOperator aggOp;
	private boolean continuous;
	private RspEngine engineType;

	public enum AggregateOperator {
		avg, min, max, sum;
	}

	public EventRequest(EventPattern ep, QosVector constraint, WeightVector weight) {
		super();
		this.ep = ep;
		this.constraint = constraint;
		this.weight = weight;
	}

	public EventPattern getEp() {
		return ep;
	}

	public void setEp(EventPattern ep) {
		this.ep = ep;
	}

	public QosVector getConstraint() {
		return constraint;
	}

	public void setConstraint(QosVector constraint) {
		this.constraint = constraint;
	}

	public WeightVector getWeight() {
		return weight;
	}

	public void setWeight(WeightVector weight) {
		this.weight = weight;
	}

	public AggregateOperator getAggOp() {
		return aggOp;
	}

	public void setAggOp(AggregateOperator aggOp) {
		this.aggOp = aggOp;
	}

	public boolean isContinuous() {
		return continuous;
	}

	public void setContinuous(boolean continuous) {
		this.continuous = continuous;
	}

	public RspEngine getEngineType() {
		return engineType;
	}

	public void setEngineType(RspEngine engineType) {
		this.engineType = engineType;
	}

	private WeightVector weight;
}
