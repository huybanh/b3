package com.betbrain.b3.model;

import java.util.HashMap;

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
		addDownlink(Outcome.PROPERTY_NAME_eventId, event);
		addDownlink(Outcome.PROPERTY_NAME_eventPartId, eventPart);
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
		boolean depthBuilding = false; 
		this.event = build(entity.getEventId(), 
				new B3Event(), Event.class, masterMap, depthBuilding);
		this.eventPart = build(entity.getEventPartId(), 
				new B3EventPart(), EventPart.class, masterMap, depthBuilding);
		
		this.status = build(entity.getStatusId(),
				new B3OutcomeStatus(), OutcomeStatus.class, masterMap, true);
		this.type = build(entity.getTypeId(),
				new B3OutcomeType(), OutcomeType.class, masterMap, true);
	}

	/*@Override
	public B3KeyOffer getB3KeyMain() {
		
		return null;
		//sportId, eventTypeId, eventPart, eventId, outcomeTypeId, outcomeId, bettingTypeId, offerId
		return new B3KeyOffer(
				event.entity.getSportId(),
				event.entity.getTypeId(),
				false,
				event.entity.getId(),
				entity.getTypeId(),
				entity.getId(),
				null,
				null);
	}*/

}
