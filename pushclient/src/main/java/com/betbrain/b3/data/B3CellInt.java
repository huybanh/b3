package com.betbrain.b3.data;

public class B3CellInt extends B3Cell<Integer> {

	public B3CellInt(String columnName, Integer value) {
		super(columnName, value);
	}

	@Override
	String getTypeName() {
		return "int";
	}

}
