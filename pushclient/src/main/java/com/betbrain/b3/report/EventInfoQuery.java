package com.betbrain.b3.report;

import java.util.ArrayList;

import com.betbrain.b3.data.B3KeyEventInfo;
import com.betbrain.b3.data.DynamoWorker;
import com.betbrain.b3.data.RevisionedEntity;
import com.betbrain.b3.model.B3Entity;
import com.betbrain.b3.pushclient.JsonMapper;

public class EventInfoQuery {
	
	private static JsonMapper mapper = new JsonMapper();
	
	@SuppressWarnings("unchecked")
	public static void main(String[] args) {
		
		DynamoWorker.initBundleCurrent();
		System.out.println("info");
		B3KeyEventInfo infoKey = new B3KeyEventInfo(219900664L, null, null, null);
		ArrayList<RevisionedEntity<?>> infos = (ArrayList<RevisionedEntity<?>>) infoKey.listEntities(true, mapper);
		for (RevisionedEntity<?> one : infos) {
			System.out.println(((B3Entity<?>) one.b3entity).entity);
		}
	}
}
