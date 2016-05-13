package com.betbrain.b3.data;

import java.util.ArrayList;

import com.amazonaws.services.dynamodbv2.document.Item;
import com.betbrain.sepc.connector.sportsmodel.Entity;

/**
 * Key spec: sportId/eventTypeId/[EVENT|EVENTPART]/eventId
 *
 */
public class B3KeyLink extends B3Key {

	final String classShortName;
	
	final long id;
	
	final String linkName;
	
	final String linkedClassShortName;
	
	final Long linkedEntityId;

	public B3KeyLink(Entity entity, Class<? extends Entity> clazz, String linkName) {
		super();
		classShortName = EntitySpec2.getShortName(entity.getClass().getName()); 
		id = entity.getId();
		linkedClassShortName = EntitySpec2.getShortName(clazz.getName());
		this.linkName = linkName;
		linkedEntityId = null;
	}

	public B3KeyLink(Class<?> entityClazz, long entityId, Class<?> linkedClazz, String linkName) {
		super();
		classShortName = EntitySpec2.getShortName(entityClazz.getName()); 
		id = entityId;
		linkedClassShortName = EntitySpec2.getShortName(linkedClazz.getName());
		this.linkName = linkName;
		linkedEntityId = null;
	}

	public B3KeyLink(Class<?> entityClazz, long entityId, Entity linkedEntity, String linkName) {
		super();
		classShortName = EntitySpec2.getShortName(entityClazz.getName()); 
		id = entityId;
		linkedClassShortName = EntitySpec2.getShortName(linkedEntity.getClass().getName());
		linkedEntityId = linkedEntity.getId();
		this.linkName = linkName;
	}

	public B3KeyLink(Entity entity, Entity linkedEntity, String linkName) {
		super();
		classShortName = EntitySpec2.getShortName(entity.getClass().getName()); 
		id = entity.getId();
		linkedClassShortName = EntitySpec2.getShortName(linkedEntity.getClass().getName());
		this.linkName = linkName;
		this.linkedEntityId = linkedEntity.getId();
		//this.linkedEntityId = linkedEntityId;
	}
	
	@Override
	B3Table getTable() {
		return B3Table.Link;
	}
	
	@Override
	boolean isDetermined() {
		return true;
	} 
	
	public String getHashKeyInternal() {
		//return classShortName + linkedClassShortName + id;
		return classShortName + linkedClassShortName + linkName + B3Table.KEY_SEP + id;
	}
	
	@Override
	String getRangeKeyInternal() {
		return String.valueOf(linkedEntityId); 
	}
	
	public ArrayList<Long> listLinks() {
		ArrayList<Long> list = new ArrayList<Long>();
		System.out.println("Querying links with hash: " + getHashKey());
		B3ItemIterator it = DynamoWorker.query(B3Table.Link, getHashKey());
		//int i = B3KeyEntity.hardLimit;
		while (it.hasNext()) {
			Item item = it.next();
			//String json = item.getString(B3Table.CELL_LOCATOR_THIZ);
			//Entity entity = JsonMapper.DeserializeF(json);
			Long linkedId = item.getLong(DynamoWorker.RANGE);
			//System.out.println(this.linkedClassShortName + ": " + linkedId);
			list.add(linkedId);
		}
		return list;
	}
}
