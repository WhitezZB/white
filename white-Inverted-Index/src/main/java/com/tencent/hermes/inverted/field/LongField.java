package com.tencent.hermes.inverted.field;

public class LongField extends Field{
	
	private Long max = Long.MIN_VALUE;
	private Long min = Long.MAX_VALUE;

	public LongField(String fieldName ) {
		super(fieldName, FieldType.INT);
	}

	@Override
	public void collectMinMax(Object v) {
		if(!(v instanceof Long)) return;
		Long tmp = (Long)v;
		if(tmp > max) {
			this.max = tmp;
		}
		if(tmp < min) {
			this.min = tmp;
		}
	}

	@Override
	public boolean inMinMax(Object v) {
		if(!(v instanceof Long)) return false;
		Long tmp = (Long)v;
		if(tmp > max || tmp < min) {
			return false;
		}
		return true;
	}

}
