package com.betbrain.b3.report;

import java.util.ArrayList;

import com.betbrain.b3.data.B3KeyEntity;
import com.betbrain.b3.data.B3KeyEventInfo;
import com.betbrain.b3.data.B3KeyOutcome;
import com.betbrain.b3.data.DynamoWorker;
import com.betbrain.b3.model.B3Outcome;
import com.betbrain.b3.pushclient.JsonMapper;
import com.betbrain.sepc.connector.sportsmodel.Event;
import com.betbrain.sepc.connector.sportsmodel.EventInfo;
import com.betbrain.sepc.connector.sportsmodel.Outcome;

public class DetailedOddsTable {
	
	private JsonMapper jsonMapper = new JsonMapper();

	public static void main(String[] args) {
		
		DynamoWorker.initBundleCurrent();
		
		//long matchId = 217410074;
		//long outcomeId = 2691102520l;
		long outcomeId = 2950462361l;
		//Y8/1/E219389552
		//Y1/1/E219387861
		//long outcomeId = 2890001650l;
		
		new DetailedOddsTable().run(outcomeId);
	}
	
	public void run(long outcomeId) {

		//TODO use lookup table
		B3KeyEntity entityKey = new B3KeyEntity(Outcome.class, outcomeId);
		Outcome outcome = (Outcome) entityKey.load(jsonMapper);
		
		entityKey = new B3KeyEntity(Event.class, outcome.getEventId());
		Event event = entityKey.load(jsonMapper);
		
		//B3KeyLookup lookupKey = new B3KeyLookup(entity, targetTable, targetHash, targetRange)
		long eventPartOrdinaryTime = 3;
		B3KeyOutcome outcomeKey = new B3KeyOutcome(event.getSportId(), event.getTypeId(), 
				event.getId(), eventPartOrdinaryTime, outcome.getTypeId(), outcome.getId());
		B3Outcome b3outcome = outcomeKey.loadFull(jsonMapper);
		
		/*long sportId = 1; //outcome.gets
		long eventId = outcome.getEventId();
		
		long eventTypeIdMatch = 0; //TODO correct me
		long eventInfoTypeIdScore = 1;
		long eventInfoTypeIdCurrentStatus = 92;*/
		
		//match statuses
		B3KeyEventInfo eventInfoKey = new B3KeyEventInfo(
				event.getSportId(), event.getTypeId(), event.getId(), 
				IDs.EVENTINFOTYPE_CURRENTSTATUS, null/*eventInfoId*/);
		ArrayList<EventInfo> matchStatuses = eventInfoKey.listEntities(jsonMapper);
		
		//scores
		eventInfoKey = new B3KeyEventInfo(
				event.getSportId(), event.getTypeId(), event.getId(), 
				IDs.EVENTINFOTYPE_SCORE, null/*eventInfoId*/);
		ArrayList<EventInfo> matchScores = eventInfoKey.listEntities(jsonMapper);
		
		//odds
		//B3KeyEntity b3key  = new B3KeyEntity(Outcome.class, outcomeId);
		//Long outcomeTypeId = b3key.load(bundle).getId();
		
		/*Long offerIdNull = null; //all offers
		Long bettingTypeIdAsianHandicap = 0l; //TODO correct me
		B3KeyOffer offerKey = new B3KeyOffer(
				event.getSportId(), event.getTypeId(), false, event.getId(), 
				outcome.getTypeId(), outcomeId, outcome.get;, offerIdNull);
		ArrayList<BettingOffer> allOffers = offerKey.listEntities();*/
	}
}
