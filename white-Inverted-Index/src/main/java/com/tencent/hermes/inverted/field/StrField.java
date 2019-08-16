package com.tencent.hermes.inverted.field;

public class StrField extends Field{

	public StrField(String fieldName) {
		super(fieldName, FieldType.STRING);
	}
	
	private String max = null;
	private String min = null;

	@Override
	public void collectMinMax(Object v) {
		if(!(v instanceof String)) return;
		String str = (String) v;
		if(max == null) {
			min = max = str;
		}else {
			int cmp = max.compareTo(str);
			if(cmp >= 0) {
				max = str;
			}else {
				cmp = min.compareTo(str);
				if(cmp > 0) {
					min = str;
				}
			}
		}
	}

	@Override
	public boolean inMinMax(Object v) {
		if(!(v instanceof String)) return false;
		String str = (String) v;
		int cmp = str.compareTo(max);
		if(cmp > 0) return false;
		cmp = str.compareTo(min);
		if(cmp < 0) return false;
		return true;
	}



}
