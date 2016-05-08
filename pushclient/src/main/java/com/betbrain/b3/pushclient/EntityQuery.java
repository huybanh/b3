package com.betbrain.b3.pushclient;

import com.betbrain.b3.data.B3KeyEntity;
import com.betbrain.b3.data.DynamoWorker;
import com.betbrain.sepc.connector.sportsmodel.BettingOffer;

public class EntityQuery {

	public static void main(String[] args) {
		DynamoWorker.initBundleCurrent();
		B3KeyEntity key = new B3KeyEntity(BettingOffer.class, 7708002332L);
		System.out.println(key.getHashKey() + "@" + key.getRangeKey());
		key.load(new JsonMapper());
	}

}
