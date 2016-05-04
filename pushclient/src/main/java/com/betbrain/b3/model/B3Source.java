package com.betbrain.b3.model;

import java.util.HashMap;

import com.betbrain.b3.pushclient.JsonMapper;
import com.betbrain.sepc.connector.sportsmodel.Entity;
import com.betbrain.sepc.connector.sportsmodel.Source;

public class B3Source extends B3Entity<Source> {

	@Override
	public void getDownlinkedEntitiesInternal() {
		
	}

	@Override
	public void buildDownlinks(HashMap<String, HashMap<Long, Entity>> masterMap, JsonMapper mapper) {
		
	}

}
