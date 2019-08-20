package com.tencent.hermes.inverted.field;

public class FloatField extends Field{
	
	private Float max = Float.MIN_VALUE;
	private Float min = Float.MAX_VALUE;

	public FloatField(String fieldName ) {
		super(fieldName, FieldType.INT);
	}

	@Override
	public void collectMinMax(Object v) {
		if(!(v instanceof Float)) return;
		Float tmp = (Float)v;
		if(tmp > max) {
			this.max = tmp;
		}
		if(tmp < min) {
			this.min = tmp;
		}
	}

	@Override
	public boolean inMinMax(Object v) {
		if(!(v instanceof Float)) return false;
		Float tmp = (Float)v;
		if(tmp > max || tmp < min) {
			return false;
		}
		return true;
	}

}
