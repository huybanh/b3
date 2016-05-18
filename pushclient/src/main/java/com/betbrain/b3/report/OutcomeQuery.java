package com.betbrain.b3.report;

import java.util.ArrayList;

import com.betbrain.b3.data.*;
import com.betbrain.b3.model.B3Outcome;
import com.betbrain.b3.pushclient.JsonMapper;

public class OutcomeQuery {

	private static JsonMapper jsonMapper = new JsonMapper();
	
	public static void main(String[] args) {
		DynamoWorker.initBundleCurrent();

		//query(219387861);
		//query(219501132);
		//query(IDs.EVENT_PREMIERLEAGUE);
		outcome(219900664L, IDs.OUTCOMETYPE_WINNER);
	}
	
	private static void outcome(long eventId, long outcomeTypeId) {
		System.out.println("Outcomes");
		B3KeyOutcome outcomeKey = new B3KeyOutcome(eventId, null, null, null);
		@SuppressWarnings("unchecked")
		ArrayList<B3Outcome> outcomes = (ArrayList<B3Outcome>) outcomeKey.listEntities(false, jsonMapper);
		for (B3Outcome one : outcomes) {
			if (one.entity.getTypeId() == 13 || one.entity.getTypeId() == 14)
			System.out.println(one.entity);
		}
	}
}
