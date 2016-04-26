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
	
	final String linkedClassShortName;
	
	final Long linkedId;

	public B3KeyLink(Entity entity, Class<? extends Entity> clazz) {
		super();
		classShortName = ModelShortName.get(entity.getClass().getName()); 
		id = entity.getId();
		linkedClassShortName = ModelShortName.get(clazz.getName());
		linkedId = null;
	}

	public B3KeyLink(Class<?> entityClazz, long entityId, Class<?> linkedClazz) {
		super();
		classShortName = ModelShortName.get(entityClazz.getName()); 
		id = entityId;
		linkedClassShortName = ModelShortName.get(linkedClazz.getName());
		linkedId = null;
	}

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
			System.out.println(this.linkedClassShortName + ": " + linkedId);
			list.add(linkedId);
			if (--i <= 0) {
				break;
			}
		}
		return list;
	}
}
