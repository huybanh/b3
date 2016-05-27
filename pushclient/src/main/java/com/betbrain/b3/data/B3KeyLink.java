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
	
	final Long id;
	
	final String linkName;
	
	final String linkedClassShortName;
	
	final Long linkedEntityId;
	
	final EntityLinkSourcePart[] sourceParts;

	public B3KeyLink(Entity entity, Class<? extends Entity> clazz, String linkName) {
		super();
		classShortName = EntitySpec2.getShortName(entity.getClass().getName()); 
		id = entity.getId();
		linkedClassShortName = EntitySpec2.getShortName(clazz.getName());
		this.linkName = linkName;
		linkedEntityId = null;
		sourceParts = null;
	}

	public B3KeyLink(Class<?> entityClazz, long entityId, Class<?> linkedClazz, String linkName) {
		super();
		classShortName = EntitySpec2.getShortName(entityClazz.getName()); 
		id = entityId;
		linkedClassShortName = EntitySpec2.getShortName(linkedClazz.getName());
		this.linkName = linkName;
		linkedEntityId = null;
		sourceParts = null;
	}

	public B3KeyLink(Class<?> entityClazz, long entityId, Entity linkedEntity, String linkName) {
		super();
		classShortName = EntitySpec2.getShortName(entityClazz.getName()); 
		id = entityId;
		linkedClassShortName = EntitySpec2.getShortName(linkedEntity.getClass().getName());
		linkedEntityId = linkedEntity.getId();
		this.linkName = linkName;
		sourceParts = null;
	}

	public B3KeyLink(Entity entity, Entity linkedEntity, String linkName) {
		super();
		classShortName = EntitySpec2.getShortName(entity.getClass().getName()); 
		id = entity.getId();
		linkedClassShortName = EntitySpec2.getShortName(linkedEntity.getClass().getName());
		this.linkName = linkName;
		this.linkedEntityId = linkedEntity.getId();
		sourceParts = null;
	}

	public B3KeyLink(Class<?> linkedEntityClazz, Long linkedEntityId, EntityLinkSourcePart... sourceParts) {
		super();
		this.classShortName = null; 
		this.id = null;
		this.linkedClassShortName = EntitySpec2.getShortName(linkedEntityClazz.getName());
		this.linkedEntityId = linkedEntityId;
		this.linkName = null;
		this.sourceParts = sourceParts;
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
		if (sourceParts == null) {
			return classShortName + linkedClassShortName + linkName + B3Table.KEY_SEP + id;
		}
		
		//cross links
		String hashKey = null;
		for (EntityLinkSourcePart p : sourceParts) {
			if (hashKey == null) {
				hashKey = "R" + B3Table.KEY_SEP;
			} else {
				hashKey += B3Table.KEY_SEP;
			}
			hashKey += EntitySpec2.getShortName(p.entityClass.getName()) + p.entityId;
		}
		return hashKey + B3Table.KEY_SEP + this.linkedClassShortName;
	}
	
	@Override
	String getRangeKeyInternal() {
		if (linkedEntityId == null) {
			return null;
		}
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
