package com.betbrain.b3.data;

import com.betbrain.sepc.connector.sportsmodel.Entity;

/**
 * Key spec: sportId/eventTypeId/[EVENT|EVENTPART]/eventId
 *
 */
public class B3KeyLink extends B3Key {

	final String classShortName;
	
	final long id;
	
	final String linkedClassShortName;
	
	final long linkedId;

	public B3KeyLink(Entity entity, Entity linkedEntity) {
		super();
		classShortName = ModelShortName.get(entity.getClass().getName()); 
		id = entity.getId();
		linkedClassShortName = ModelShortName.get(linkedEntity.getClass().getName());
		linkedId = linkedEntity.getId();
	}
	
	@Override
	boolean isDetermined() {
		return true;
	} 
	
	protected String getHashKey() {
		return classShortName + linkedClassShortName + id;
	}
	
	@Override
	String getRangeKey() {
		return String.valueOf(linkedId); 
	}
}
