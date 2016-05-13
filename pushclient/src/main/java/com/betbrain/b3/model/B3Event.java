package com.betbrain.b3.model;

import java.util.HashMap;

import com.betbrain.b3.data.B3KeyEvent;
import com.betbrain.b3.data.EntitySpec2;
import com.betbrain.b3.pushclient.JsonMapper;
import com.betbrain.sepc.connector.sportsmodel.Entity;
import com.betbrain.sepc.connector.sportsmodel.Event;
import com.betbrain.sepc.connector.sportsmodel.EventPart;
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
	public EntitySpec2 getSpec() {
		return EntitySpec2.Event;
	}

	@Override
	public void getDownlinkedEntitiesInternal() {
		
		//unfollowed-link: same type
		addDownlinkUnfollowed(Event.PROPERTY_NAME_parentId, Event.class/*, entity.getParentId()*/);
		addDownlinkUnfollowed(Event.PROPERTY_NAME_parentPartId, EventPart.class/*, entity.getParentPartId()*/);
		addDownlinkUnfollowed(Event.PROPERTY_NAME_currentPartId, EventPart.class/*, entity.getCurrentPartId()*/);
		addDownlinkUnfollowed(Event.PROPERTY_NAME_rootPartId, EventPart.class/*, entity.getRootPartId()*/);
		
		addDownlink(Event.PROPERTY_NAME_sportId, Sport.class, sport); 
		addDownlink(Event.PROPERTY_NAME_statusId, EventStatus.class, status);
		addDownlink(Event.PROPERTY_NAME_templateId, EventTemplate.class, template);
		//addDownlink(Event.PROPERTY_NAME_venueId, linkedEntity), //no venue entity
		addDownlink(Event.PROPERTY_NAME_typeId, EventType.class, type);
	}

	@Override
	public void buildDownlinks(boolean forMainKeyOnly, HashMap<String, HashMap<Long, Entity>> masterMap,
			JsonMapper mapper) {
		
		if (forMainKeyOnly) {
			return;
		}
		this.sport = build(forMainKeyOnly, entity.getSportId(), new B3Sport(), 
				Sport.class, masterMap, mapper);
		this.status = build(forMainKeyOnly, entity.getStatusId(), new B3EventStatus(), 
				EventStatus.class, masterMap, mapper);
		this.template = build(forMainKeyOnly, entity.getTemplateId(), new B3EventTemplate(), 
				EventTemplate.class, masterMap, mapper);
		this.type = build(forMainKeyOnly, entity.getTypeId(), new B3EventType(),
				EventType.class, masterMap, mapper);
	}
	
	@Override
	String canCreateMainKey() {
		if (entity == null) {
			return "Null entity";
		}
		return null;
	}

	@Override
	B3KeyEvent createMainKey() {
		if (entity == null) {
			return null;
		}
		return new B3KeyEvent(entity.getSportId(), entity.getTypeId(), entity.getId());
		
	}

}
