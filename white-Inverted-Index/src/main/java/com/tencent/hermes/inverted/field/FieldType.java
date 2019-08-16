package com.tencent.hermes.inverted.field;

public enum FieldType {
	
	LONG(0),
	INT(1),
	STRING(2),
	DOUBLE(3),
	FLOAT(4);
	
	private int index;
	
	private FieldType(int index) {
		this.index = index;
	}
	public int getIndex() {  
        return this.index;  
    } 
}
