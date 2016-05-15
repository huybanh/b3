package com.betbrain.b3.model;

import com.betbrain.sepc.connector.sportsmodel.Entity;

public class ItemProvider {

	private String provider;

	private String value;
	
	public String getProvider() {
		return provider;
	}
	
	public void setProvider(String provider) {
		this.provider = provider;
	}
	
	public String getValue() {
		return value;
	}
	
	public void setValue(String value) {
		this.value = value;
	}
	
	//Constructor
	
	public ItemProvider() {
		
	}
	
	public ItemProvider(String provider, String value) {
		this.provider = provider;
		this.value = value;
	}
}
