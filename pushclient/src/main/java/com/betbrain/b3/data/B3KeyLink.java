package com.betbrain.b3.data;

import java.util.ArrayList;

import com.amazonaws.services.dynamodbv2.document.Item;
import com.amazonaws.services.dynamodbv2.document.ItemCollection;
import com.amazonaws.services.dynamodbv2.document.QueryOutcome;
import com.amazonaws.services.dynamodbv2.document.internal.IteratorSupport;
import com.betbrain.sepc.connector.sportsmodel.Entity;

/**
 * Key spec: sportId/eventTypeId/[EVENT|EVENTPART]/eventId
 *
 */
public class B3KeyLink extends B3Key {

	final String classShortName;
	
	final long id;
	
	final String linkName;
	
	//final String linkedClassShortName;
	
	final Long linkedEntityId;

	public B3KeyLink(Entity entity, /*Class<? extends Entity> clazz*/String linkName) {
		super();
		classShortName = ModelShortName.get(entity.getClass().getName()); 
		id = entity.getId();
		//linkedClassShortName = ModelShortName.get(clazz.getName());
		this.linkName = linkName;
		linkedEntityId = null;
	}

	public B3KeyLink(Class<?> entityClazz, long entityId, /*Class<?> linkedClazz*/String linkName) {
		super();
		classShortName = ModelShortName.get(entityClazz.getName()); 
		id = entityId;
		//linkedClassShortName = ModelShortName.get(linkedClazz.getName());
		this.linkName = linkName;
		linkedEntityId = null;
	}

	public B3KeyLink(Entity entity, String linkName, long linkedEntityId) {
		super();
		classShortName = ModelShortName.get(entity.getClass().getName()); 
		id = entity.getId();
		//linkedClassShortName = ModelShortName.get(linkedEntity.getClass().getName());
		this.linkName = linkName;
		//linkedId = linkedEntity.getId();
		this.linkedEntityId = linkedEntityId;
	}
	
	@Override
	boolean isDetermined() {
		return true;
	} 
	
	protected String getHashKey() {
		//return classShortName + linkedClassShortName + id;
		return classShortName + linkName + id;
	}
	
	@Override
	String getRangeKey() {
		return String.valueOf(linkedEntityId); 
	}
	
	public ArrayList<Long> listLinks() {
		ArrayList<Long> list = new ArrayList<Long>();
		ItemCollection<QueryOutcome> coll = DynamoWorker.query(B3Table.Link, getHashKey());
		IteratorSupport<Item, QueryOutcome> it = coll.iterator();
		int i = B3KeyEntity.hardLimit;
		while (it.hasNext()) {
			Item item = it.next();
			//String json = item.getString(B3Table.CELL_LOCATOR_THIZ);
			//Entity entity = JsonMapper.DeserializeF(json);
			Long linkedId = item.getLong("range");
			System.out.println(this.linkName + ": " + linkedId);
			list.add(linkedId);
			if (--i <= 0) {
				break;
			}
		}
		return list;
	}
}
