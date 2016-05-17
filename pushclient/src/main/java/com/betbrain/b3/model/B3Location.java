package com.betbrain.b3.model;

import java.util.HashMap;

import com.amazonaws.services.dynamodbv2.document.Item;
import com.betbrain.b3.data.EntitySpec2;
import com.betbrain.b3.pushclient.JsonMapper;
import com.betbrain.sepc.connector.sportsmodel.Entity;
import com.betbrain.sepc.connector.sportsmodel.Event;
import com.betbrain.sepc.connector.sportsmodel.Location;
import com.betbrain.sepc.connector.sportsmodel.LocationType;

public class B3Location extends B3Entity<Location> {
	
	public B3LocationType locationType;

	@Override
	public EntitySpec2 getSpec() {
		return EntitySpec2.Location;
	}

	@Override
	protected void getDownlinkedEntitiesInternal() {
		addDownlink(Location.PROPERTY_NAME_typeId, LocationType.class, locationType);
	}

	@Override
	public void buildDownlinks(boolean forMainKeyOnly, 
			HashMap<String, HashMap<Long, Entity>> masterMap, JsonMapper mapper) {
		
		this.locationType = build(forMainKeyOnly, entity.getTypeId(), 
				new B3LocationType(), LocationType.class, masterMap, mapper);
	}

	@Override
	public void load(Item item, JsonMapper mapper) {
		super.load(item, null, mapper);
		locationType = new B3LocationType();
		locationType.load(item, Event.PROPERTY_NAME_typeId, mapper);
	}

}