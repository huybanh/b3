package com.betbrain.b3.model;

import java.util.HashMap;

import com.betbrain.b3.pushclient.JsonMapper;
import com.betbrain.sepc.connector.sportsmodel.Entity;

public abstract class B3EntityLeaf<E extends Entity> extends B3Entity<E> {

	@Override
	protected void getDownlinkedEntitiesInternal() {
		//no downlinks
	}

	@Override
	public void buildDownlinks(boolean forMainKeyOnly, HashMap<String, HashMap<Long, Entity>> masterMap, JsonMapper mapper) {
		//no downlinks
	}

	/*@Override
	public void load(Item item, JsonMapper mapper) {
		super.load(item, null, mapper);
	}*/

}
