package com.betbrain.b3.model;

import java.util.HashMap;

import com.amazonaws.services.dynamodbv2.document.Item;
import com.betbrain.b3.data.B3KeyOutcome;
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
		addDownlinkUnfollowed(Outcome.PROPERTY_NAME_eventId, Event.class/*, entity.getEventId()*/);
		
		//followed
		addDownlink(Outcome.PROPERTY_NAME_eventPartId, eventPart);
		addDownlink(Outcome.PROPERTY_NAME_statusId, status);
		addDownlink(Outcome.PROPERTY_NAME_typeId, type);
	}

	@Override
	public void buildDownlinks(boolean forMainKeyOnly, HashMap<String, HashMap<Long, Entity>> masterMap, JsonMapper mapper) {
		
		this.event = build(forMainKeyOnly, entity.getEventId(), 
				new B3Event(), Event.class, masterMap, mapper);
		if (forMainKeyOnly) {
			return;
		}
		this.eventPart = build(forMainKeyOnly, entity.getEventPartId(), 
				new B3EventPart(), EventPart.class, masterMap, mapper);
		this.status = build(forMainKeyOnly, entity.getStatusId(),
				new B3OutcomeStatus(), OutcomeStatus.class, masterMap, mapper);
		this.type = build(forMainKeyOnly, entity.getTypeId(),
				new B3OutcomeType(), OutcomeType.class, masterMap, mapper);
	}
	
	public void loadFull(Item item, JsonMapper mapper) {
		deserialize(mapper, item, B3Table.CELL_LOCATOR_THIZ);
		this.status = new B3OutcomeStatus();
		this.status.deserialize(mapper, item, Outcome.PROPERTY_NAME_statusId);
		this.type = new B3OutcomeType();
		this.type.deserialize(mapper, item, Outcome.PROPERTY_NAME_typeId);
	}

	@Override
	B3KeyOutcome createMainKey() {
		if (entity == null || event == null) {
			return null;
		}
		return new B3KeyOutcome(event.entity.getSportId(), event.entity.getTypeId(), event.entity.getId(),
				entity.getEventPartId(), entity.getTypeId(), entity.getId());
		
	}

}
