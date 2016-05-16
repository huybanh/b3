package com.betbrain.b3.data;

import java.util.ArrayList;

import com.amazonaws.services.dynamodbv2.document.Item;
import com.betbrain.b3.model.B3Entity;
import com.betbrain.b3.pushclient.JsonMapper;
import com.betbrain.sepc.connector.sportsmodel.Entity;

public abstract class B3MainKey<E extends Entity> extends B3Key {
	
	abstract EntitySpec2 getEntitySpec();

	public ArrayList<?> listEntities(boolean revisions, JsonMapper jsonMapper ) {
		
		String hashKeySuffix;
		if (revisions) {
			hashKeySuffix = B3Table.KEY_SUFFIX_REVISION;
		} else {
			hashKeySuffix = "";
		}

		@SuppressWarnings("rawtypes")
		ArrayList list = new ArrayList();
		String hashKey = getHashKeyInternal();
		if (hashKey == null) {
			for (int i = 0; i < B3Table.DIST_FACTOR; i++) {
				query(i + hashKeySuffix, 1, revisions, list, jsonMapper);
			}
		} else {
			query(hashKey + hashKeySuffix, null, revisions, list, jsonMapper);
		}
		
		return list;
	}
	
	@SuppressWarnings("unchecked")
	private void query(String hashKey, Integer partitionRecordsLimit, boolean revisions, 
			@SuppressWarnings("rawtypes") ArrayList list, JsonMapper jsonMapper) {
		Class<? extends B3Entity<?>> b3class = getEntitySpec().b3class;
		B3ItemIterator it = DynamoWorker.query(getTable(), hashKey, getRangeKey(), partitionRecordsLimit);
		
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
	}

}
