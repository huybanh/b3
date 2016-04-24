package com.betbrain.b3.model;

import java.util.HashMap;

import com.betbrain.b3.data.EntityLink;
import com.betbrain.sepc.connector.sportsmodel.BettingType;
import com.betbrain.sepc.connector.sportsmodel.Entity;

public class B3BettingType extends B3Entity<BettingType> {

	@Override
	public EntityLink[] getDownlinkedEntities() {
		return null;
	}

	@Override
	public void buildDownlinks(HashMap<String, HashMap<Long, Entity>> masterMap) {
		
	}

}
