package com.betbrain.b3.data;

import java.util.HashMap;
import java.util.LinkedList;

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
import com.betbrain.b3.pushclient.EntityUpdateWrapper;

public enum EntitySpec2 {

	//Short names are exact-two-character 
	BettingOffer("BO", B3Table.BettingOffer, true, B3BettingOffer.class,
			com.betbrain.sepc.connector.sportsmodel.BettingOffer.class.getName()),
	BettingOfferStatus("BS", null, false, B3BettingOfferStatus.class,
			com.betbrain.sepc.connector.sportsmodel.BettingOfferStatus.class.getName()),
	BettingOfferType("BT", null, false, B3BettingType.class,
			com.betbrain.sepc.connector.sportsmodel.BettingType.class.getName()),
	Event("EV", B3Table.Event, false, B3Event.class,
			com.betbrain.sepc.connector.sportsmodel.Event.class.getName()),
	EventInfo("EI", B3Table.EventInfo, true, B3EventInfo.class,
			com.betbrain.sepc.connector.sportsmodel.EventInfo.class.getName()),
	EventInfoType("EF", null, false, B3EventInfoType.class,
			com.betbrain.sepc.connector.sportsmodel.EventInfoType.class.getName()),
	EventPart("EP", null, false, B3EventPart.class,
			com.betbrain.sepc.connector.sportsmodel.EventPart.class.getName()),
	EventStatus("ES", null, false, B3EventStatus.class,
			com.betbrain.sepc.connector.sportsmodel.EventStatus.class.getName()),
	EventTemplate("EM", null, false, B3EventTemplate.class, 
			com.betbrain.sepc.connector.sportsmodel.EventTemplate.class.getName()),
	EventType("ET", null, false, B3EventType.class,
			com.betbrain.sepc.connector.sportsmodel.EventType.class.getName()),
	Outcome("OC", B3Table.Outcome, false, B3Outcome.class,
			com.betbrain.sepc.connector.sportsmodel.Outcome.class.getName()),
	OutcomeStatus("OS", null, false, B3OutcomeStatus.class,
			com.betbrain.sepc.connector.sportsmodel.OutcomeStatus.class.getName()),
	OutcomeType("OT", null, false, B3OutcomeType.class,
			com.betbrain.sepc.connector.sportsmodel.OutcomeType.class.getName()),
	Provider("PR", null, false, B3Provider.class,
			com.betbrain.sepc.connector.sportsmodel.Provider.class.getName()),
	Source("SO", null, false, B3Source.class,
			com.betbrain.sepc.connector.sportsmodel.Source.class.getName()),
	Sport("SP", null, false, B3Sport.class,
			com.betbrain.sepc.connector.sportsmodel.Sport.class.getName());
	
	public final String shortName;
	public final B3Table mainTable;
	public final boolean revisioned;
	public final Class<? extends B3Entity<?>> b3class;
	public final String entityClassName;
	
	private EntitySpec2(String shortName, B3Table mainTable, boolean revisioned,
			Class<? extends B3Entity<?>> b3class, String entityClassName) {
		
		this.shortName = shortName;
		this.mainTable = mainTable;
		this.revisioned = revisioned;
		this.b3class = b3class;
		this.entityClassName = entityClassName;
	}
	
	private static HashMap<String, EntitySpec2> allShortNames;
	
	static void initialize() {
		allShortNames = new HashMap<String, EntitySpec2>();
		for (EntitySpec2 em : EntitySpec2.values()) {
			allShortNames.put(em.entityClassName, em);
		}
	}
	
	public static EntitySpec2 get(String entityClassName) {
		return allShortNames.get(entityClassName);
	}
	
	public static String getShortName(String entityClassName) {
		EntitySpec2 n = allShortNames.get(entityClassName);
		if (n == null) {
			return null;
		}
		return n.shortName;
	}
	
	private LinkedList<String> idPropertyNames;
	
	public boolean isStructuralChange(EntityUpdateWrapper update) {
		if (idPropertyNames == null) {
			idPropertyNames = new LinkedList<>();
			EntityLink[] links;
			try {
				links = ((B3Entity<?>) b3class.newInstance()).getDownlinkedNames();
			} catch (InstantiationException | IllegalAccessException e) {
				throw new RuntimeException(e);
			}
			for (EntityLink one : links) {
				idPropertyNames.add(one.getLinkName());
			}
		}
		for (String changedPropertyName : update.getPropertyNames()) {
			if (idPropertyNames.contains(changedPropertyName)) {
				return true;
			}
		}
		return false;
	}

}
