package com.tencent.hermes.inverted.field;

public abstract class Field {
	
	public Field(String fieldName, FieldType type) {
		this.fieldName = fieldName;
		this.type = type;
	}
	
	public String fieldName;
	
	public FieldType type;
	
	public abstract void collectMinMax(Object v);
	
	public abstract boolean inMinMax(Object v);
}
