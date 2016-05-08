package com.betbrain.b3.data;

public class B3CellInt extends B3Cell<Integer> {

	public B3CellInt(String columnName, Integer value) {
		super(columnName, value);
	}
	
	public B3CellInt() {
		super(null, null);
	}

	@Override
	String getTypeName() {
		return "int";
	}

	public Integer getValue() {
		return this.value;
	}
	
	public void setValue(Integer i) {
		this.value = i;
	}
}
