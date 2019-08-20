package com.tencent.hermes.inverted.field;

public class DoubleField extends Field{
	
	private Double max = Double.MIN_VALUE;
	private Double min = Double.MAX_VALUE;

	public DoubleField(String fieldName ) {
		super(fieldName, FieldType.INT);
	}

	@Override
	public void collectMinMax(Object v) {
		if(!(v instanceof Double)) return;
		Double tmp = (Double)v;
		if(tmp > max) {
			this.max = tmp;
		}
		if(tmp < min) {
			this.min = tmp;
		}
	}

	@Override
	public boolean inMinMax(Object v) {
		if(!(v instanceof Double)) return false;
		Double tmp = (Double)v;
		if(tmp > max || tmp < min) {
			return false;
		}
		return true;
	}

}
