package com.betbrain.b3.report;

import java.util.ArrayList;

import com.betbrain.b3.data.B3KeyEntity;
import com.betbrain.b3.data.B3KeyLink;
import com.betbrain.b3.data.DynamoWorker;
import com.betbrain.b3.pushclient.JsonMapper;
import com.betbrain.sepc.connector.sportsmodel.*;

public class OutcomeQuery {

	private static JsonMapper jsonMapper = new JsonMapper();
	
	public static void main(String[] args) {
		DynamoWorker.initBundleCurrent();

		//query(219387861);
		//query(219501132);
		//query(IDs.EVENT_PREMIERLEAGUE);
		match(217562668L);
	}
	
	private static void match(long matchId) {
		//match
		B3KeyEntity keyEntity = new B3KeyEntity(Event.class, matchId);
		Event match = keyEntity.load(jsonMapper);
		System.out.println("Match: " + match);
		
		//outcomes
		B3KeyLink keyLink = new B3KeyLink(Event.class, matchId, Outcome.class, Outcome.PROPERTY_NAME_eventId);
		ArrayList<Long> outcomeIds = keyLink.listLinks();
		System.out.println("Outcome count: " + outcomeIds.size());
		
	}
}
