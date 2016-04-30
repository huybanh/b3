package com.betbrain.b3.model;

import java.util.HashMap;
import java.util.LinkedList;

import com.betbrain.b3.data.B3Bundle;
import com.betbrain.b3.data.B3Cell;
import com.betbrain.b3.data.B3CellString;
import com.betbrain.b3.data.B3KeyEntity;
import com.betbrain.b3.data.B3KeyEventInfo;
import com.betbrain.b3.data.B3Table;
import com.betbrain.b3.data.B3Update;
import com.betbrain.b3.data.DynamoWorker;
import com.betbrain.b3.data.InitialDumpDeployer;
import com.betbrain.b3.pushclient.EntityCreateWrapper;
import com.betbrain.b3.pushclient.EntityDeleteWrapper;
import com.betbrain.b3.pushclient.EntityUpdateWrapper;
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
	public void buildDownlinks(HashMap<String, HashMap<Long, Entity>> masterMap, B3Bundle bundle, JsonMapper mapper) {
		this.provider = build(entity.getProviderId(), new B3Provider(),
				Provider.class, masterMap, bundle, mapper);
		this.source = build(entity.getSourceId(), new B3Source(), 
				Source.class, masterMap, bundle, mapper);
		this.type = build(entity.getTypeId(), new B3EventInfoType(), 
				EventInfoType.class, masterMap, bundle, mapper);
		
		//need to link to event, but skip in b3 db
		this.event = build(entity.getEventId(), new B3Event(), 
				Event.class, masterMap, bundle, mapper);
		this.eventPart = build(entity.getEventPartId(), new B3EventPart(), 
				EventPart.class, masterMap, bundle, mapper);
	}

	@Override
	void applyChangeCreate(EntityCreateWrapper create, B3Bundle bundle, JsonMapper mapper) {
		
		//table entity
		super.applyChangeCreate(create, bundle, mapper);
		
		//load event for more details
		EventInfo newEventInfo = (EventInfo) create.getEntity();
		B3KeyEntity entityKey = new B3KeyEntity(Event.class, newEventInfo.getEventId());
		Event targetEvent = entityKey.load(bundle, mapper);
		if (targetEvent == null) {
			System.out.println("Ignoring event info due to missing target event: " + newEventInfo.getId());
			return;
		}

		System.out.println("Well, event info found");
		buildDownlinks(null, bundle, mapper);
		LinkedList<B3Cell<?>> b3Cells = new LinkedList<B3Cell<?>>();
		boolean eventPart = false; //TODO correct this
		B3KeyEventInfo infoKey = new B3KeyEventInfo(targetEvent.getSportId(), 
				targetEvent.getTypeId(), eventPart, newEventInfo.getEventId(), 
				newEventInfo.getTypeId(), newEventInfo.getId());
		
		//put linked entities to table lookup, link
		InitialDumpDeployer.putToMainAndLookupAndLinkRecursively(B3Table.EventInfo, 
				infoKey, b3Cells, null, this, null, bundle, mapper);
		
		//put main entity to main table
		B3Update update = new B3Update(B3Table.EventInfo, infoKey, b3Cells.toArray(new B3CellString[b3Cells.size()]));
		DynamoWorker.put(bundle, update);
		
	}

	@Override
	void applyChangeUpdate(EntityUpdateWrapper update, B3Bundle bundle, JsonMapper mapper) {
		
	}

	@Override
	void applyChangeDelete(EntityDeleteWrapper delte, B3Bundle bundle, JsonMapper mapper) {
		
	}

}
