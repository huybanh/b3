package com.betbrain.b3.data;

import java.util.ArrayList;

import com.amazonaws.services.dynamodbv2.document.Item;
import com.betbrain.b3.model.B3Entity;
import com.betbrain.b3.pushclient.JsonMapper;
import com.betbrain.sepc.connector.sportsmodel.Entity;

public abstract class B3MainKey<E extends Entity> extends B3Key {
	
	abstract EntitySpec2 getEntitySpec();

	@SuppressWarnings("unchecked")
	public ArrayList<?> listEntities(boolean revisions, JsonMapper jsonMapper ) {
		
		String hashKey;
		if (revisions) {
			hashKey = getHashKeyInternal() + B3Table.KEY_SUFFIX_REVISION;
		} else {
			hashKey = getHashKeyInternal();
		}
		
		/*ArrayList<E> list = new ArrayList<E>();
		B3ItemIterator it = DynamoWorker.query(getTable(), hashKey, getRangeKey(), null);
		Class<? extends B3Entity<?>> b3class = getEntitySpec().b3class;
		while (it.hasNext()) {
			Item item = it.next();
			B3Entity<?> b3entity;
			try {
				b3entity = b3class.newInstance();
			} catch (InstantiationException | IllegalAccessException e) {
				throw new RuntimeException(e);
			}
			b3entity.load(item);
		}*/
		
		@SuppressWarnings("rawtypes")
		ArrayList list = new ArrayList();
		Class<? extends B3Entity<?>> b3class = getEntitySpec().b3class;
		B3ItemIterator it = DynamoWorker.query(getTable(), hashKey, getRangeKey(), null);
		
		while (it.hasNext()) {
			Item item = it.next();
			B3Entity<Entity> b3entity;
			try {
				b3entity = (B3Entity<Entity>) b3class.newInstance();
			} catch (InstantiationException | IllegalAccessException e) {
				throw new RuntimeException(e);
			}
			//b3entity.entity = entity;
			b3entity.load(item, jsonMapper);
			if (revisions) {
				String rangeKey = item.getString(DynamoWorker.RANGE);
				long revisionTime = Long.parseLong(rangeKey.substring(rangeKey.lastIndexOf('/') + 1));
				list.add(new RevisionedEntity<>(revisionTime, b3entity));
			} else {
				String json = item.getString(B3Table.CELL_LOCATOR_THIZ);
				E entity = (E) jsonMapper.deserialize(json);
				b3entity.entity = entity;
				list.add(b3entity);
			}
		}
		return list;
	}

}
