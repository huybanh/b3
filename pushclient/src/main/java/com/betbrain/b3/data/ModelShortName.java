package com.betbrain.b3.data;

import java.util.HashMap;

public enum ModelShortName {

	//Short names are exact-two-character 
	BettingOffer("BO", com.betbrain.sepc.connector.sportsmodel.BettingOffer.class.getName()),
	BettingOfferStatus("BS", com.betbrain.sepc.connector.sportsmodel.BettingOfferStatus.class.getName()),
	BettingOfferType("BT", com.betbrain.sepc.connector.sportsmodel.BettingType.class.getName()),
	Event("EV", com.betbrain.sepc.connector.sportsmodel.Event.class.getName()),
	EventInfo("EI", com.betbrain.sepc.connector.sportsmodel.EventInfo.class.getName()),
	EventInfoType("EF", com.betbrain.sepc.connector.sportsmodel.EventInfoType.class.getName()),
	EventPart("EP", com.betbrain.sepc.connector.sportsmodel.EventPart.class.getName()),
	EventStatus("ES", com.betbrain.sepc.connector.sportsmodel.EventStatus.class.getName()),
	EventTemplate("EM", com.betbrain.sepc.connector.sportsmodel.EventTemplate.class.getName()),
	EventType("ET", com.betbrain.sepc.connector.sportsmodel.EventType.class.getName()),
	Outcome("OC", com.betbrain.sepc.connector.sportsmodel.Outcome.class.getName()),
	OutcomeStatus("OS", com.betbrain.sepc.connector.sportsmodel.OutcomeStatus.class.getName()),
	OutcomeType("OT", com.betbrain.sepc.connector.sportsmodel.OutcomeType.class.getName()),
	Provider("PR", com.betbrain.sepc.connector.sportsmodel.Provider.class.getName()),
	Source("SO", com.betbrain.sepc.connector.sportsmodel.Source.class.getName()),
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
		ModelShortName n = allShortNames.get(entityClassName);
		if (n == null) {
			return null;
		}
		return n.shortName;
		//return allShortNames.get(entityClassName).shortName;
	}

}
