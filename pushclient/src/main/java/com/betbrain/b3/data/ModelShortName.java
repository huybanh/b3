package com.betbrain.b3.data;

import java.util.HashMap;

public enum ModelShortName {

	//Short names are exact-two-character 
	BettingOffer("BO", com.betbrain.sepc.connector.sportsmodel.BettingOffer.class.getName()),
	Outcome("OC", com.betbrain.sepc.connector.sportsmodel.Outcome.class.getName()),
	
	Event("EV", com.betbrain.sepc.connector.sportsmodel.Event.class.getName()),
	Sport("SP", com.betbrain.sepc.connector.sportsmodel.Sport.class.getName());
	
	public final String shortName;
	public final String className;
	
	private ModelShortName(String shortName, String className) {
		this.shortName = shortName;
		this.className = className;
	}
	
	private static HashMap<String, ModelShortName> allShortNames;
	
	public static void initialize() {
		allShortNames = new HashMap<String, ModelShortName>();
		for (ModelShortName em : ModelShortName.values()) {
			allShortNames.put(em.className, em);
		}
	}
	
	public static String get(String entityClassName) {
		return allShortNames.get(entityClassName).shortName;
	}

}
