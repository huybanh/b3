package com.betbrain.b3.data;

import java.util.ArrayList;

import com.amazonaws.services.dynamodbv2.document.Item;
import com.amazonaws.services.dynamodbv2.document.ItemCollection;
import com.amazonaws.services.dynamodbv2.document.QueryOutcome;
import com.amazonaws.services.dynamodbv2.document.internal.IteratorSupport;
import com.betbrain.b3.pushclient.JsonMapper;
import com.betbrain.sepc.connector.sportsmodel.Entity;

public abstract class B3KeyEntitySupport extends B3Key {
	
	static final int hardLimit = 50;
	
	//private JsonMapper jsonMapper = new JsonMapper();
	
	@SuppressWarnings("unchecked")
	public <E extends Entity> ArrayList<E> listEntities(B3Bundle bundle, JsonMapper jsonMapper ) {
		ArrayList<E> list = new ArrayList<E>();
		int i = hardLimit;
		ItemCollection<QueryOutcome> coll = DynamoWorker.query(bundle, B3Table.Entity, getHashKey());
		IteratorSupport<Item, QueryOutcome> it = coll.iterator();
		while (it.hasNext()) {
			if (--i <= 0) {
				break;
			}
			Item item = it.next();
			String json = item.getString(B3Table.CELL_LOCATOR_THIZ);
			Entity entity = jsonMapper.deserializeEntity(json);
			System.out.println(entity);
			list.add((E) entity);
		}
		return list;
	}
}
