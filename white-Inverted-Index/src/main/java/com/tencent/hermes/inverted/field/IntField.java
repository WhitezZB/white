package com.tencent.hermes.inverted.field;

public class IntField extends Field{
	
	private Integer max = Integer.MIN_VALUE;
	private Integer min = Integer.MAX_VALUE;

	public IntField(String fieldName ) {
		super(fieldName, FieldType.INT);
	}

	@Override
	public void collectMinMax(Object v) {
		if(!(v instanceof Integer)) return;
		Integer tmp = (Integer)v;
		if(tmp > max) {
			this.max = tmp;
		}
		if(tmp < min) {
			this.min = tmp;
		}
	}

	@Override
	public boolean inMinMax(Object v) {
		if(!(v instanceof Integer)) return false;
		Integer tmp = (Integer)v;
		if(tmp > max || tmp < min) {
			return false;
		}
		return true;
	}

}
