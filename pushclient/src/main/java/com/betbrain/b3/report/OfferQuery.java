package com.betbrain.b3.report;

import java.util.ArrayList;

import com.betbrain.b3.data.B3KeyOffer;
import com.betbrain.b3.data.DynamoWorker;
import com.betbrain.b3.pushclient.JsonMapper;
import com.betbrain.sepc.connector.sportsmodel.BettingOffer;

public class OfferQuery {
	
	private static JsonMapper mapper = new JsonMapper();
	
	public static void main(String[] args) {
		
		//DynamoWorker.initBundleCurrent();
		DynamoWorker.initBundleByStatus("SPRINT2");
		offer(217562668L, IDs.OUTCOME_WINNER, 2954860246L, IDs.BETTINGTYPE_1X2);
	}
	
	private static void offer(long eventId, long outcomeTypeId, long outcomeId, long bettingTypeId) {
		System.out.println("Offers");
		B3KeyOffer offerKey = new B3KeyOffer(1L, 1L, eventId, outcomeTypeId, outcomeId, bettingTypeId, null);
		ArrayList<BettingOffer> offers = offerKey.listEntities(true, mapper);
		for (BettingOffer one : offers) {
			System.out.println(one);
		}
	}
}
