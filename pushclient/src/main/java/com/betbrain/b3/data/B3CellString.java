package com.betbrain.b3.data;

public class B3CellString extends B3Cell<String> {

	public B3CellString(String columnName, String value) {
		super(columnName, value);
	}
	
	public B3CellString() {
		super(null, null);
	}

	@Override
	String getTypeName() {
		return "String";
	}

	public String getValue() {
		return this.value;
	}
	
	public void setValue(String s) {
		this.value = s;
	}

}
