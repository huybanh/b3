package com.betbrain.b3.model;

import java.util.HashMap;

import com.betbrain.sepc.connector.sportsmodel.Entity;
import com.betbrain.sepc.connector.sportsmodel.Event;
import com.betbrain.sepc.connector.sportsmodel.EventStatus;
import com.betbrain.sepc.connector.sportsmodel.EventTemplate;
import com.betbrain.sepc.connector.sportsmodel.EventType;
import com.betbrain.sepc.connector.sportsmodel.Sport;

public class B3Event extends B3Entity<Event> {

	public B3Sport sport;
	public B3EventStatus status;
	public B3EventTemplate template;
	public B3EventType type;

	@Override
	public void getDownlinkedEntitiesInternal() {
		
		//unfollowed-link: same type
		//addDownlink(Event.PROPERTY_NAME_currentPartId, currentPart),
		//addDownlink(Event.PROPERTY_NAME_rootPartId, linkedEntity),
		
		addDownlink(Event.PROPERTY_NAME_sportId, sport); 
		addDownlink(Event.PROPERTY_NAME_statusId, status);
		addDownlink(Event.PROPERTY_NAME_templateId, template);
		//addDownlink(Event.PROPERTY_NAME_venueId, linkedEntity), //no venue entity
		addDownlink(Event.PROPERTY_NAME_typeId, type);
	}

	@Override
	public void buildDownlinks(HashMap<String, HashMap<Long, Entity>> masterMap) {
		this.sport = build(entity.getSportId(), new B3Sport(), Sport.class, masterMap);
		this.status = build(entity.getStatusId(), new B3EventStatus(), EventStatus.class, masterMap);
		this.template = build(entity.getTemplateId(), new B3EventTemplate(), EventTemplate.class, masterMap);
		this.type = build(entity.getTypeId(), new B3EventType(), EventType.class, masterMap);
	}

}
