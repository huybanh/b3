package com.betbrain.b3.model;

import java.util.HashMap;

import com.betbrain.b3.data.EntityLink;
import com.betbrain.sepc.connector.sportsmodel.Entity;
import com.betbrain.sepc.connector.sportsmodel.Source;

public class B3Source extends B3Entity<Source> {

	@Override
	public EntityLink[] getDownlinkedEntities() {
		return null;
	}

	@Override
	public void buildDownlinks(HashMap<String, HashMap<Long, Entity>> masterMap) {
		
	}

}
