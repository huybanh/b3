package com.betbrain.b3.model;

import java.util.HashMap;

import com.betbrain.b3.data.EntityLink;
import com.betbrain.sepc.connector.sportsmodel.Entity;
import com.betbrain.sepc.connector.sportsmodel.Outcome;

public class B3Outcome extends B3Entity<Outcome> {

	public B3Event event;
	
	/*protected B3Outcome(Outcome entity) {
		super(entity);
	}*/

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

	@Override
	public EntityLink[] getDownlinkedEntities() {
		return new EntityLink[] {new EntityLink(Outcome.PROPERTY_NAME_eventId, event)};
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
		this.event = build(entity.getEventId(), new B3Event(), masterMap, depthBuilding);
	}

}
