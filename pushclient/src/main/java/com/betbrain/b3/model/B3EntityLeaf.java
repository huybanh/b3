package com.betbrain.b3.model;

import java.util.HashMap;

import com.betbrain.sepc.connector.sportsmodel.Entity;

public class B3EntityLeaf<E extends Entity> extends B3Entity<E> {

	@Override
	protected void getDownlinkedEntitiesInternal() {
		//no downlinks
	}

	@Override
	public void buildDownlinks(HashMap<String, HashMap<Long, Entity>> masterMap) {
		//no downlinks
	}

}
