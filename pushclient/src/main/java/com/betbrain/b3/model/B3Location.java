package com.betbrain.b3.model;

import java.util.HashMap;

import com.amazonaws.services.dynamodbv2.document.Item;
import com.betbrain.b3.data.B3Table;
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
		locationType = loadChild(new B3LocationType(), item, baseCellName + Event.PROPERTY_NAME_typeId, mapper);
		return true;
	}

}
