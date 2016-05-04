package com.betbrain.b3.data;

import java.util.HashMap;

import com.betbrain.b3.model.B3BettingOffer;
import com.betbrain.b3.model.B3BettingOfferStatus;
import com.betbrain.b3.model.B3BettingType;
import com.betbrain.b3.model.B3Entity;
import com.betbrain.b3.model.B3Event;
import com.betbrain.b3.model.B3EventInfo;
import com.betbrain.b3.model.B3EventInfoType;
import com.betbrain.b3.model.B3EventPart;
import com.betbrain.b3.model.B3EventStatus;
import com.betbrain.b3.model.B3EventTemplate;
import com.betbrain.b3.model.B3EventType;
import com.betbrain.b3.model.B3Outcome;
import com.betbrain.b3.model.B3OutcomeStatus;
import com.betbrain.b3.model.B3OutcomeType;
import com.betbrain.b3.model.B3Provider;
import com.betbrain.b3.model.B3Source;
import com.betbrain.b3.model.B3Sport;

public enum ModelShortName {

	//Short names are exact-two-character 
	BettingOffer("BO", B3BettingOffer.class, com.betbrain.sepc.connector.sportsmodel.BettingOffer.class.getName()),
	BettingOfferStatus("BS", B3BettingOfferStatus.class, com.betbrain.sepc.connector.sportsmodel.BettingOfferStatus.class.getName()),
	BettingOfferType("BT", B3BettingType.class, com.betbrain.sepc.connector.sportsmodel.BettingType.class.getName()),
	Event("EV", B3Event.class, com.betbrain.sepc.connector.sportsmodel.Event.class.getName()),
	EventInfo("EI", B3EventInfo.class, com.betbrain.sepc.connector.sportsmodel.EventInfo.class.getName()),
	EventInfoType("EF", B3EventInfoType.class, com.betbrain.sepc.connector.sportsmodel.EventInfoType.class.getName()),
	EventPart("EP", B3EventPart.class, com.betbrain.sepc.connector.sportsmodel.EventPart.class.getName()),
	EventStatus("ES", B3EventStatus.class, com.betbrain.sepc.connector.sportsmodel.EventStatus.class.getName()),
	EventTemplate("EM", B3EventTemplate.class, com.betbrain.sepc.connector.sportsmodel.EventTemplate.class.getName()),
	EventType("ET", B3EventType.class, com.betbrain.sepc.connector.sportsmodel.EventType.class.getName()),
	Outcome("OC", B3Outcome.class, com.betbrain.sepc.connector.sportsmodel.Outcome.class.getName()),
	OutcomeStatus("OS", B3OutcomeStatus.class, com.betbrain.sepc.connector.sportsmodel.OutcomeStatus.class.getName()),
	OutcomeType("OT", B3OutcomeType.class, com.betbrain.sepc.connector.sportsmodel.OutcomeType.class.getName()),
	Provider("PR", B3Provider.class, com.betbrain.sepc.connector.sportsmodel.Provider.class.getName()),
	Source("SO", B3Source.class, com.betbrain.sepc.connector.sportsmodel.Source.class.getName()),
	Sport("SP", B3Sport.class, com.betbrain.sepc.connector.sportsmodel.Sport.class.getName());
	
	public final String shortName;
	public final String className;
	public final Class<? extends B3Entity<?>> b3class;
	
	private ModelShortName(String shortName, Class<? extends B3Entity<?>> b3class, String className) {
		this.shortName = shortName;
		this.className = className;
		this.b3class = b3class;
	}
	
	private static HashMap<String, ModelShortName> allShortNames;
	
	static void initialize() {
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
	
	public static Class<? extends B3Entity<?>> getB3Class(String entityClassName) {
		ModelShortName n = allShortNames.get(entityClassName);
		if (n == null) {
			return null;
		}
		return n.b3class;
		//return allShortNames.get(entityClassName).shortName;
	}

}
