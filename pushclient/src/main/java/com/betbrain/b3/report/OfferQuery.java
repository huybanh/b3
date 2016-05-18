package com.betbrain.b3.report;

import java.util.ArrayList;

import com.betbrain.b3.data.B3KeyOffer;
import com.betbrain.b3.data.DynamoWorker;
import com.betbrain.b3.data.RevisionedEntity;
import com.betbrain.b3.model.B3BettingOffer;
import com.betbrain.b3.pushclient.JsonMapper;

public class OfferQuery {
	
	private static JsonMapper mapper = new JsonMapper();
	
	public static void main(String[] args) {
		
		DynamoWorker.initBundleCurrent();
		offer(217562668L, IDs.OUTCOMETYPE_WINNER, 2954860246L, IDs.BETTINGTYPE_1X2);
	}
	
	private static void offer(long eventId, long outcomeTypeId, long outcomeId, long bettingTypeId) {
		System.out.println("Offers");
		B3KeyOffer offerKey = new B3KeyOffer(219900664L, 3L, 14L, 3044603660L, 47L, null);
		@SuppressWarnings("unchecked")
		ArrayList<RevisionedEntity<B3BettingOffer>> offers = 
				(ArrayList<RevisionedEntity<B3BettingOffer>>) offerKey.listEntities(true, mapper);
		for (RevisionedEntity<B3BettingOffer> one : offers) {
			System.out.println(one.b3entity.entity);
		}
	}
}
