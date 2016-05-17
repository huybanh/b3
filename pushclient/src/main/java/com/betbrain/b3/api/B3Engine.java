package com.betbrain.b3.api;

import java.util.ArrayList;

import com.betbrain.b3.data.B3KeyEntity;
import com.betbrain.b3.data.DynamoWorker;
import com.betbrain.b3.model.B3Sport;
import com.betbrain.b3.pushclient.JsonMapper;
import com.betbrain.sepc.connector.sportsmodel.Sport;

public class B3Engine { 
	
	public static void main(String[] args) {
		B3Engine b3 = new B3Engine();
		b3.getSports();
	}

	public B3Engine() {
		DynamoWorker.initBundleCurrent();
	}
	
	public Sport[] getSports() {
		JsonMapper jsonMapper = new JsonMapper();
		B3KeyEntity entityKey = new B3KeyEntity(Sport.class);
		ArrayList<?> allSports = entityKey.listEntities(false, B3Sport.class, jsonMapper);
		Sport[] result = new Sport[allSports.size()];
		int index = 0;
		for (Object one : allSports) {
			result[index] = ((B3Sport) one).entity;
			System.out.println(result[index]);
			index++;
		}
		return result;
	}
}
