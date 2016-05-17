package com.betbrain.b3.report;

import com.betbrain.b3.data.B3KeyEntity;
import com.betbrain.b3.data.DynamoWorker;
import com.betbrain.b3.pushclient.JsonMapper;
import com.betbrain.sepc.connector.sportsmodel.*;

public class EntityQuery {

	public static void main(String[] args) {
		DynamoWorker.initBundleCurrent();
		JsonMapper mapper = new JsonMapper();
		//B3KeyEntity keyEntity = new B3KeyEntity(Event.class, 217562679L);
		B3KeyEntity keyEntity = new B3KeyEntity(Outcome.class, 2383565601L);
		Entity e = keyEntity.load(mapper);
		System.out.println(e);
	}

}
