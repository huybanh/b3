package com.betbrain.b3.data;

public class B3CellLong extends B3Cell<Long> {

	public B3CellLong(String columnName, Long value) {
		super(columnName, value);
	}
	
	public B3CellLong() {
		super(null, null);
	}

	@Override
	String getTypeName() {
		return "long";
	}

	public Long getValue() {
		return this.value;
	}
	
	public void setValue(Long i) {
		this.value = i;
	}

}
