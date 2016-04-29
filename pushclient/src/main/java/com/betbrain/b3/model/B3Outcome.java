package com.betbrain.b3.model;

import java.util.HashMap;

import com.amazonaws.services.dynamodbv2.document.Item;
import com.betbrain.b3.data.B3Table;
import com.betbrain.b3.pushclient.JsonMapper;
import com.betbrain.sepc.connector.sportsmodel.Entity;
import com.betbrain.sepc.connector.sportsmodel.Event;
import com.betbrain.sepc.connector.sportsmodel.EventPart;
import com.betbrain.sepc.connector.sportsmodel.Outcome;
import com.betbrain.sepc.connector.sportsmodel.OutcomeStatus;
import com.betbrain.sepc.connector.sportsmodel.OutcomeType;

public class B3Outcome extends B3Entity<Outcome> {

	public B3Event event;
	public B3EventPart eventPart;
	public B3OutcomeStatus status;
	public B3OutcomeType type;

	@Override
	public void getDownlinkedEntitiesInternal() {
		
		//unfollowed links
		addDownlink(Outcome.PROPERTY_NAME_eventId, Event.class, entity.getEventId());
		addDownlink(Outcome.PROPERTY_NAME_eventPartId, EventPart.class, entity.getEventPartId());
		
		addDownlink(Outcome.PROPERTY_NAME_statusId, status);
		addDownlink(Outcome.PROPERTY_NAME_typeId, type);
	}

	@Override
	public void buildDownlinks(HashMap<String, HashMap<Long, Entity>> masterMap) {
		/*HashMap<Long, Entity> allEvents = masterMap.get(Event.class.getName());
		Event one = (Event) allEvents.get(entity.getEventId());
		this.event = new B3Event(one);*/
		
		//we don't want event graph going into BettingOffer table
		//this.event.buildDownlinks(masterMap);
		
		//we don't want event graph going into BettingOffer table: depthBuilding = false
		//boolean depthBuilding = false; 
		this.event = build(entity.getEventId(), 
				new B3Event(), Event.class, masterMap, true);
		this.eventPart = build(entity.getEventPartId(), 
				new B3EventPart(), EventPart.class, masterMap, true);
		
		this.status = build(entity.getStatusId(),
				new B3OutcomeStatus(), OutcomeStatus.class, masterMap, true);
		this.type = build(entity.getTypeId(),
				new B3OutcomeType(), OutcomeType.class, masterMap, true);
	}
	
	public void loadFull(Item item) {
		JsonMapper mapper = new JsonMapper();
		deserialize(mapper, item, this, B3Table.CELL_LOCATOR_THIZ);
		this.status = (B3OutcomeStatus) deserialize(mapper, item, new B3OutcomeStatus(), Outcome.PROPERTY_NAME_statusId);
		this.type = (B3OutcomeType) deserialize(mapper, item, new B3OutcomeType(), Outcome.PROPERTY_NAME_typeId);
	}

}
