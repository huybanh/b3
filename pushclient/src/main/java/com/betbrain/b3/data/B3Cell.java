package com.betbrain.b3.data;

public abstract class B3Cell<T> {
	
	String columnName;
	
	T value;
	
	public B3Cell() {
		//for flexjson
	}

	public B3Cell(String columnName, T value) {
		super();
		this.columnName = columnName;
		this.value = value;
	}
	
	abstract String getTypeName();
	
	public String getColumnName() {
		return this.columnName;
	}
	
	public void setColumnName(String name) {
		this.columnName = name;
	}
}
