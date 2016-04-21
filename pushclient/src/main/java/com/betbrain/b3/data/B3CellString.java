package com.betbrain.b3.data;

public class B3CellString extends B3Cell<String> {

	public B3CellString(String columnName, String value) {
		super(columnName, value);
	}

	@Override
	String getTypeName() {
		return "String";
	}

}
