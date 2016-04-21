package com.betbrain.b3.data;

abstract class B3Cell<T> {
	
	final String columnName;
	
	final T value;

	public B3Cell(String columnName, T value) {
		super();
		this.columnName = columnName;
		this.value = value;
	}
	
	abstract String getTypeName();
}
