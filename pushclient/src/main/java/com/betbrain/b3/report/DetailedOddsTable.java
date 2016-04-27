package com.betbrain.b3.report;

import java.util.ArrayList;

import com.betbrain.b3.data.B3KeyEntity;
import com.betbrain.b3.data.B3KeyEvent;
import com.betbrain.b3.data.B3KeyEventInfo;
import com.betbrain.b3.data.B3KeyLink;
import com.betbrain.b3.data.B3KeyOffer;
import com.betbrain.b3.data.DynamoWorker;
import com.betbrain.b3.data.ModelShortName;
import com.betbrain.sepc.connector.sportsmodel.BettingOffer;
import com.betbrain.sepc.connector.sportsmodel.Event;
import com.betbrain.sepc.connector.sportsmodel.EventInfo;
import com.betbrain.sepc.connector.sportsmodel.EventPart;
import com.betbrain.sepc.connector.sportsmodel.EventType;
import com.betbrain.sepc.connector.sportsmodel.Outcome;
import com.betbrain.sepc.connector.sportsmodel.OutcomeType;
import com.betbrain.sepc.connector.sportsmodel.Sport;

public class DetailedOddsTable {

	public static void main(String[] args) {
		
		ModelShortName.initialize();
		DynamoWorker.initialize();
		
		//B3KeyEntity key  = new B3KeyEntity(EventPart.class, 1);
		//key.listEntities();
		//key.load();
		
		B3KeyLink key = new B3KeyLink(Sport.class, IDs.SPORT_FOOTBALL, Event.class, "sportId");
		ArrayList<Long> ids = key.listLinks();
		B3KeyEntity.load(Event.class, ids);
		
		//B3KeyEvent key = new B3KeyEvent(IDs.SPORT_FOOTBALL, eventTypeId, eventPart, eventId)
		//b3key.listEntities();

		long eventId = 1; //input
		Long outcomeId1x2 = 1l; //TODO correct me
		//new DetailedOddsTable().run(eventId, outcomeId1x2);
	}
	
	public void run(long eventId, long outcomeId) {

		long sportIdFootball = 1;
		long eventTypeIdMatch = 0; //TODO correct me
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
		B3KeyEntity b3key  = new B3KeyEntity(Outcome.class, outcomeId);
		Long outcomeTypeId = b3key.load().getId();
		
		Long offerIdNull = null; //all offers
		Long bettingTypeIdAsianHandicap = 0l; //TODO correct me
		B3KeyOffer offerKey = new B3KeyOffer(
				sportIdFootball, eventTypeIdMatch, false, eventId, 
				outcomeTypeId, outcomeId, bettingTypeIdAsianHandicap, offerIdNull);
		ArrayList<BettingOffer> allOffers = offerKey.listEntities();
	}
}
