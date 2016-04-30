package com.betbrain.b3.report;

import java.util.ArrayList;

import com.betbrain.b3.data.B3KeyEntity;
import com.betbrain.b3.data.B3KeyLink;
import com.betbrain.b3.data.DynamoWorker;
import com.betbrain.b3.data.ModelShortName;
import com.betbrain.b3.pushclient.JsonMapper;
import com.betbrain.sepc.connector.sportsmodel.BettingOffer;
import com.betbrain.sepc.connector.sportsmodel.Entity;
import com.betbrain.sepc.connector.sportsmodel.Event;
import com.betbrain.sepc.connector.sportsmodel.EventInfo;
import com.betbrain.sepc.connector.sportsmodel.EventInfoType;
import com.betbrain.sepc.connector.sportsmodel.EventStatus;
import com.betbrain.sepc.connector.sportsmodel.EventType;
import com.betbrain.sepc.connector.sportsmodel.Outcome;
import com.betbrain.sepc.connector.sportsmodel.OutcomeType;
import com.betbrain.sepc.connector.sportsmodel.Sport;

public class QueryTests {
	
	public static void main(String[] args) {
		ModelShortName.initialize();
		DynamoWorker.initialize();
		
		//all sports
		//ArrayList<Entity> sports = new B3KeyEntity(Sport.class).listEntities();

		DynamoWorker.initBundleCurrent(); 
		JsonMapper jsonMapper = new JsonMapper();
		ArrayList<Long> ids;
		B3KeyLink keyLink = new B3KeyLink(Event.class, 206795928, Outcome.class, "eventId");
		ArrayList<Long> outcomeIds = keyLink.listLinks();
		keyLink = new B3KeyLink(Outcome.class, outcomeIds.get(0), BettingOffer.class, "outcomeId");
		keyLink.listLinks();
		//ids = new B3KeyLink(sports.get(0), Event.class).listLinks();
		//B3KeyEntity.load(Event.class, ids);
		
		//all event types
		//new B3KeyEntity(EventType.class).listEntities();
		
		//all event statuses
		//new B3KeyEntity(EventStatus.class).listEntities();

		//new B3KeyEntity(OutcomeType.class).listEntities(bundle, jsonMapper);
		//new B3KeyEntity(EventInfoType.class).listEntities();
		//new B3KeyEntity(EventInfo.class).listEntities();
		//B3KeyEntity.load(EventInfoType.class, 92);
		
		//event to outcome
		//ids = new B3KeyLink(Event.class, 217409474, Outcome.class).listLinks();
		//B3KeyEntity.load(Outcome.class, ids);
		
		//outcome to offer
		//new B3KeyLink(Outcome.class, 2890125761l, BettingOffer.class).listLinks();
	}
}
