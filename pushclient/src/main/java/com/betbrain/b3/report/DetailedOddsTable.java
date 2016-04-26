package com.betbrain.b3.report;

import java.util.ArrayList;

import com.betbrain.b3.data.B3KeyEntity;
import com.betbrain.b3.data.B3KeyEventInfo;
import com.betbrain.b3.data.B3KeyOffer;
import com.betbrain.b3.data.DynamoWorker;
import com.betbrain.b3.data.ModelShortName;
import com.betbrain.sepc.connector.sportsmodel.BettingOffer;
import com.betbrain.sepc.connector.sportsmodel.EventInfo;
import com.betbrain.sepc.connector.sportsmodel.Outcome;

public class DetailedOddsTable {

	public static void main(String[] args) {
		
		ModelShortName.initialize();
		DynamoWorker.initialize();
		
		long sportIdFootball = 1;
		long eventTypeIdMatch = 0; //TODO correct me
		long eventId = 1; //input
		long eventInfoTypeIdScore = 1;
		long eventInfoTypeIdCurrentStatus = 92;
		
		//match statuses
		B3KeyEventInfo eventInfoKey = new B3KeyEventInfo(
				sportIdFootball, eventTypeIdMatch, false, eventId, eventInfoTypeIdCurrentStatus, null/*eventInfoId*/);
		ArrayList<EventInfo> matchStatuses = eventInfoKey.listEntities();
		
		//scores
		eventInfoKey = new B3KeyEventInfo(
				sportIdFootball, eventTypeIdMatch, false, eventId, eventInfoTypeIdScore, null/*eventInfoId*/);
		ArrayList<EventInfo> matchScores = eventInfoKey.listEntities();
		
		//odds
		Long outcomeId1x2 = 1l; //TODO correct me
		B3KeyEntity b3key  = new B3KeyEntity(Outcome.class, outcomeId1x2);
		Long outcomeTypeId = b3key.load().getId();
		
		Long bettingTypeIdAsianHandicap = 0l; //TODO correct me
		Long offerIdNull = null; //all offers
		B3KeyOffer offerKey = new B3KeyOffer(
				sportIdFootball, eventTypeIdMatch, false, eventId, 
				outcomeTypeId, outcomeId1x2, bettingTypeIdAsianHandicap, offerIdNull);
		ArrayList<BettingOffer> allOffers = offerKey.listEntities();
	}
}
