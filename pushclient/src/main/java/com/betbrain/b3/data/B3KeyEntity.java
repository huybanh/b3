package com.betbrain.b3.data;

import com.betbrain.sepc.connector.sportsmodel.Entity;

/**
 * Key spec: sportId/eventTypeId/[EVENT|EVENTPART]/eventId
 *
 */
public class B3KeyEntity extends B3Key {

	final String classShortName;
	
	final long id;

	public B3KeyEntity(Entity entity) {
		super();
		classShortName = ModelShortName.get(entity.getClass().getName()); 
		id = entity.getId();
	}
	
	@Override
	boolean isDetermined() {
		return true;
	} 
	
	protected String getHashKey() {
		return classShortName + Math.abs(((Long) id).hashCode() % 100);
	}
	
	@Override
	String getRangeKey() {
		return String.valueOf(id); 
	}
}
