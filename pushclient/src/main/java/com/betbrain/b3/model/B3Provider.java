package com.betbrain.b3.model;

import java.util.HashMap;

import com.betbrain.b3.data.EntityLink;
import com.betbrain.sepc.connector.sportsmodel.Entity;
import com.betbrain.sepc.connector.sportsmodel.Provider;

public class B3Provider extends B3Entity<Provider> {

	/*public B3Provider(Provider entity) {
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
