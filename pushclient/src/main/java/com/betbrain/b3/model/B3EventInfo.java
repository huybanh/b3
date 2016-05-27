package com.betbrain.b3.model;

import java.util.HashMap;

import com.amazonaws.services.dynamodbv2.document.Item;
import com.betbrain.b3.data.B3KeyEventInfo;
import com.betbrain.b3.data.B3Table;
import com.betbrain.b3.data.EntitySpec2;
import com.betbrain.b3.pushclient.JsonMapper;
import com.betbrain.sepc.connector.sportsmodel.Entity;
import com.betbrain.sepc.connector.sportsmodel.Event;
import com.betbrain.sepc.connector.sportsmodel.EventInfo;
import com.betbrain.sepc.connector.sportsmodel.EventInfoType;
import com.betbrain.sepc.connector.sportsmodel.EventPart;
import com.betbrain.sepc.connector.sportsmodel.Provider;

public class B3EventInfo extends B3Entity<EventInfo> {

	public B3Provider provider;
	//public B3Source source;
	public B3EventInfoType type;
	
	//package private
	public B3Event event;
	public B3EventPart eventPart;
	
	@Override
	public EntitySpec2 getSpec() {
		return EntitySpec2.EventInfo;
	}

	@Override
	public boolean load(Item item, String cellName, JsonMapper mapper) {
		if (!super.load(item, cellName, mapper)) {
			return false;
		}
		String baseCellName;
		if (cellName == null) {
			baseCellName = "";
		} else {
			baseCellName = cellName + B3Table.CELL_LOCATOR_SEP;
		}
		provider = loadChild(new B3Provider(), item, baseCellName + EventInfo.PROPERTY_NAME_providerId, mapper);
		return true;
	}

	@Override
	public void getDownlinkedEntitiesInternal() {
		
		//skip event/eventpart
		addDownlinkUnfollowed(EventInfo.PROPERTY_NAME_eventId, Event.class/*, entity.getEventId()*/);
		addDownlinkUnfollowed(EventInfo.PROPERTY_NAME_eventPartId, EventPart.class/*, entity.getEventPartId()*/);
		
		addDownlink(EventInfo.PROPERTY_NAME_providerId, Provider.class, provider);
		//addDownlink(EventInfo.PROPERTY_NAME_sourceId, Source.class, source);
		addDownlink(EventInfo.PROPERTY_NAME_typeId, EventInfoType.class, type);
	}

	@Override
	public void buildDownlinks(boolean forMainKeyOnly, HashMap<String, HashMap<Long, Entity>> masterMap, JsonMapper mapper) {

		this.event = build(forMainKeyOnly, entity.getEventId(), new B3Event(), 
				Event.class, masterMap, mapper);
		
		if (forMainKeyOnly) {
			return;
		}
		this.provider = build(forMainKeyOnly, entity.getProviderId(), new B3Provider(),
				Provider.class, masterMap, mapper);
		//this.source = build(forMainKeyOnly, entity.getSourceId(), new B3Source(), 
		//		Source.class, masterMap, mapper);
		this.type = build(forMainKeyOnly, entity.getTypeId(), new B3EventInfoType(), 
				EventInfoType.class, masterMap, mapper);
		
		this.eventPart = build(forMainKeyOnly, entity.getEventPartId(), new B3EventPart(), 
				EventPart.class, masterMap, mapper);
	}
	
	@Override
	String canCreateMainKey() {
		if (entity == null) {
			return "Null entity";
		}
		if (event == null) {
			return "Missing event " + entity.getEventId();
		}
		return null;
	}

	@Override
	public B3KeyEventInfo createMainKey() {
		if (entity == null || event == null) {
			return null;
		}
		EventInfo info = (EventInfo) entity;
		return new B3KeyEventInfo(/*event.entity.getId(), event.entity.getTypeId(),*/ 
				info.getEventId(), info.getEventPartId(), info.getTypeId(), info.getId());
		
	}
	
	@Override
	String getRevisionId() {
		return null;
	}
}
