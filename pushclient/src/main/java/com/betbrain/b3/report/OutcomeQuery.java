package com.betbrain.b3.report;

import java.util.ArrayList;

import com.betbrain.b3.data.*;
import com.betbrain.b3.pushclient.JsonMapper;
import com.betbrain.sepc.connector.sportsmodel.*;

public class OutcomeQuery {

	private static JsonMapper jsonMapper = new JsonMapper();
	
	public static void main(String[] args) {
		DynamoWorker.initBundleCurrent();

		//query(219387861);
		//query(219501132);
		//query(IDs.EVENT_PREMIERLEAGUE);
		outcome(217562668L, IDs.OUTCOME_WINNER);
	}
	
	private static void outcome(long eventId, long outcomeTypeId) {
		System.out.println("Offers");
		B3KeyOutcome outcomeKey = new B3KeyOutcome(1L, 1L, eventId, IDs.EVENTPART_ORDINARYTIME, outcomeTypeId, null);
		@SuppressWarnings("unchecked")
		ArrayList<Outcome> outcomes = (ArrayList<Outcome>) outcomeKey.listEntities(false, jsonMapper);
		for (Outcome one : outcomes) {
			System.out.println(one);
		}
	}
}
