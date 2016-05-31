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
import com.betbrain.b3.model.B3EventParticipantRelation;
import com.betbrain.b3.model.B3EventStatus;
import com.betbrain.b3.model.B3EventTemplate;
import com.betbrain.b3.model.B3EventType;
import com.betbrain.b3.model.B3Location;
import com.betbrain.b3.model.B3LocationType;
import com.betbrain.b3.model.B3Outcome;
import com.betbrain.b3.model.B3OutcomeStatus;
import com.betbrain.b3.model.B3OutcomeType;
import com.betbrain.b3.model.B3OutcomeTypeBettingTypeRelation;
import com.betbrain.b3.model.B3Participant;
import com.betbrain.b3.model.B3Provider;
import com.betbrain.b3.model.B3Source;
import com.betbrain.b3.model.B3Sport;
import com.betbrain.sepc.connector.sportsmodel.Entity;
import com.betbrain.sepc.connector.sportsmodel.EntityUpdate;

public enum EntitySpec2 {

	//Short names are exact-two-character 
	BettingOffer("BO", B3Table.BettingOffer, true, B3BettingOffer.class,
			com.betbrain.sepc.connector.sportsmodel.BettingOffer.class),
	BettingOfferStatus("BS", null, false, B3BettingOfferStatus.class,
			com.betbrain.sepc.connector.sportsmodel.BettingOfferStatus.class),
	BettingOfferType("BT", null, false, B3BettingType.class,
			com.betbrain.sepc.connector.sportsmodel.BettingType.class),
	Event("EV", B3Table.Event, false, B3Event.class,
			com.betbrain.sepc.connector.sportsmodel.Event.class),
	EventInfo("EI", B3Table.EventInfo, true, B3EventInfo.class,
			com.betbrain.sepc.connector.sportsmodel.EventInfo.class),
	EventInfoType("EF", null, false, B3EventInfoType.class,
			com.betbrain.sepc.connector.sportsmodel.EventInfoType.class),
	EventPart("EP", null, false, B3EventPart.class,
			com.betbrain.sepc.connector.sportsmodel.EventPart.class),
	EventParticipantRelation("ER", B3Table.EPRelation, false, B3EventParticipantRelation.class,
			com.betbrain.sepc.connector.sportsmodel.EventParticipantRelation.class),
	EventStatus("ES", null, false, B3EventStatus.class,
			com.betbrain.sepc.connector.sportsmodel.EventStatus.class),
	EventTemplate("EM", null, false, B3EventTemplate.class, 
			com.betbrain.sepc.connector.sportsmodel.EventTemplate.class),
	EventType("ET", null, false, B3EventType.class,
			com.betbrain.sepc.connector.sportsmodel.EventType.class),
	Location("LO", null, false, B3Location.class,
			com.betbrain.sepc.connector.sportsmodel.Location.class),
	LocationType("LT", null, false, B3LocationType.class,
			com.betbrain.sepc.connector.sportsmodel.LocationType.class),
	OutcomeTypeBettingTypeRelation("OB", null, false, B3OutcomeTypeBettingTypeRelation.class,
			com.betbrain.sepc.connector.sportsmodel.OutcomeTypeBettingTypeRelation.class),
	Outcome("OC", B3Table.Outcome, false, B3Outcome.class,
			com.betbrain.sepc.connector.sportsmodel.Outcome.class),
	OutcomeStatus("OS", null, false, B3OutcomeStatus.class,
			com.betbrain.sepc.connector.sportsmodel.OutcomeStatus.class),
	OutcomeType("OT", null, false, B3OutcomeType.class,
			com.betbrain.sepc.connector.sportsmodel.OutcomeType.class),
	Participant("PA", null, false, B3Participant.class,
			com.betbrain.sepc.connector.sportsmodel.Participant.class),
	Provider("PR", null, false, B3Provider.class,
			com.betbrain.sepc.connector.sportsmodel.Provider.class),
	Source("SO", null, false, B3Source.class,
			com.betbrain.sepc.connector.sportsmodel.Source.class),
	Sport("SP", null, false, B3Sport.class,
			com.betbrain.sepc.connector.sportsmodel.Sport.class);
	
	public final String shortName;
	public final B3Table mainTable;
	public final boolean revisioned;
	public final Class<? extends B3Entity<?>> b3class;
	public final Class<? extends Entity> entityClass;
	
	private EntitySpec2(String shortName, B3Table mainTable, boolean revisioned,
			Class<? extends B3Entity<?>> b3class, Class<? extends Entity> entityClass) {
		
		this.shortName = shortName;
		this.mainTable = mainTable;
		this.revisioned = revisioned;
		this.b3class = b3class;
		this.entityClass = entityClass;
	}
	
	private static HashMap<String, EntitySpec2> allShortNames;
	
	public static void initialize() {
		allShortNames = new HashMap<String, EntitySpec2>();
		for (EntitySpec2 em : EntitySpec2.values()) {
			allShortNames.put(em.entityClass.getName(), em);
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
	
	public boolean isStructuralChange(EntityUpdate update) {
		if (idPropertyNames == null) {
			idPropertyNames = new LinkedList<>();
			try {
				B3Entity<?> b3e = (B3Entity<?>) b3class.newInstance();
				/*if (b3e.isKeyChange(update)) {
					return true;
				}*/
				EntityLink[] links = b3e.getDownlinkedNames();
				for (EntityLink one : links) {
					idPropertyNames.add(one.getLinkName());
				}
			} catch (InstantiationException | IllegalAccessException e) {
				throw new RuntimeException(e);
			}
		}
		for (String changedPropertyName : update.getPropertyNames()) {
			if (idPropertyNames.contains(changedPropertyName)) {
				return true;
			}
		}
		return false;
	}

	B3Entity<?> newB3Entity() {
		try {
			return b3class.newInstance();
		} catch (InstantiationException | IllegalAccessException e) {
			throw new RuntimeException(e);
		}
	}
}
