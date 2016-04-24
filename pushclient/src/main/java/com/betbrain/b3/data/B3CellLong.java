package com.betbrain.b3.data;

public class B3CellLong extends B3Cell<Long> {

	public B3CellLong(String columnName, Long value) {
		super(columnName, value);
	}

	@Override
	String getTypeName() {
		return "long";
	}

}
