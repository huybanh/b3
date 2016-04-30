package com.betbrain.b3.data;

public class B3Bundle {

	final String id;
	
	B3Bundle(String id) {
		this.id = id;
	}

	@Override
	public String toString() {
		return id;
	}
}
