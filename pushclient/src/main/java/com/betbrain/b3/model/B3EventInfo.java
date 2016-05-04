package com.betbrain.b3.model;

import java.util.HashMap;

import com.betbrain.b3.data.B3KeyEventInfo;
import com.betbrain.b3.pushclient.JsonMapper;
import com.betbrain.sepc.connector.sportsmodel.Entity;
import com.betbrain.sepc.connector.sportsmodel.Event;
import com.betbrain.sepc.connector.sportsmodel.EventInfo;
import com.betbrain.sepc.connector.sportsmodel.EventInfoType;
import com.betbrain.sepc.connector.sportsmodel.EventPart;
import com.betbrain.sepc.connector.sportsmodel.Provider;
import com.betbrain.sepc.connector.sportsmodel.Source;

public class B3EventInfo extends B3Entity<EventInfo> {

	public B3Provider provider;
	public B3Source source;
	public B3EventInfoType type;
	
	//package private
	public B3Event event;
	public B3EventPart eventPart;

	@Override
	public void getDownlinkedEntitiesInternal() {
		
		//skip event/eventpart
		addDownlink(EventInfo.PROPERTY_NAME_eventId, Event.class, entity.getEventId());
		addDownlink(EventInfo.PROPERTY_NAME_eventPartId, EventPart.class, entity.getEventPartId());
		
		addDownlink(EventInfo.PROPERTY_NAME_providerId, provider);
		addDownlink(EventInfo.PROPERTY_NAME_sourceId, source);
		addDownlink(EventInfo.PROPERTY_NAME_typeId, type);
	}

	@Override
	public void buildDownlinks(HashMap<String, HashMap<Long, Entity>> masterMap, JsonMapper mapper) {
		this.provider = build(entity.getProviderId(), new B3Provider(),
				Provider.class, masterMap, mapper);
		this.source = build(entity.getSourceId(), new B3Source(), 
				Source.class, masterMap, mapper);
		this.type = build(entity.getTypeId(), new B3EventInfoType(), 
				EventInfoType.class, masterMap, mapper);
		
		//need to link to event, but skip in b3 db
		this.event = build(entity.getEventId(), new B3Event(), 
				Event.class, masterMap, mapper);
		this.eventPart = build(entity.getEventPartId(), new B3EventPart(), 
				EventPart.class, masterMap, mapper);
	}

	@Override
	B3KeyEventInfo createMainKey() {
		EventInfo info = (EventInfo) entity;
		return new B3KeyEventInfo(event.entity.getId(), event.entity.getTypeId(), 
				info.getEventId(), info.getTypeId(), info.getId());
		
	}

}
