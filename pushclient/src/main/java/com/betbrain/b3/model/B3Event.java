package com.betbrain.b3.model;

import java.util.HashMap;

import com.betbrain.b3.data.EntityLink;
import com.betbrain.sepc.connector.sportsmodel.Entity;
import com.betbrain.sepc.connector.sportsmodel.Event;

public class B3Event extends B3Entity<Event> {

	/*public B3Event(Event entity) {
		super(entity);
	}*/

	@Override
	public EntityLink[] getDownlinkedEntities() {
		return null;
	}

	@Override
	public void buildDownlinks(HashMap<String, HashMap<Long, Entity>> masterMap) {
		
	}

}
