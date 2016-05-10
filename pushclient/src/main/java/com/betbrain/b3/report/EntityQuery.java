package com.betbrain.b3.report;

import com.betbrain.b3.data.B3KeyEntity;
import com.betbrain.b3.data.DynamoWorker;
import com.betbrain.b3.pushclient.JsonMapper;
import com.betbrain.sepc.connector.sportsmodel.Outcome;

public class EntityQuery {

	public static void main(String[] args) {
		DynamoWorker.initBundleCurrent();
		JsonMapper mapper = new JsonMapper();
		B3KeyEntity keyEntity = new B3KeyEntity(Outcome.class, 3020356094L);
		keyEntity.load(mapper);
	}

}
