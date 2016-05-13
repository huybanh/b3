package com.betbrain.b3.data;

import java.util.ArrayList;

import com.amazonaws.services.dynamodbv2.document.Item;
import com.betbrain.b3.pushclient.JsonMapper;

public abstract class B3MainKey<E> extends B3Key {

	@SuppressWarnings("unchecked")
	public ArrayList<E> listEntities(boolean revisions, JsonMapper jsonMapper ) {
		
		String hashKey;
		if (revisions) {
			hashKey = getHashKeyInternal() + B3Table.KEY_SUFFIX_REVISION;
		} else {
			hashKey = getHashKeyInternal();
		}
		ArrayList<E> list = new ArrayList<E>();
		B3ItemIterator it = DynamoWorker.query(getTable(), hashKey, getRangeKey(), null);
		while (it.hasNext()) {
			Item item = it.next();
			String json = item.getString(B3Table.CELL_LOCATOR_THIZ);
			E entity = (E) jsonMapper.deserialize(json);
			//System.out.println(entity);
			list.add(entity);
		}
		return list;
	}

}
