package com.betbrain.b3.data;

import java.util.ArrayList;

import com.betbrain.sepc.connector.sportsmodel.BettingOffer;
import com.betbrain.sepc.connector.sportsmodel.Entity;
import com.betbrain.sepc.connector.sportsmodel.Event;
import com.betbrain.sepc.connector.sportsmodel.EventInfo;
import com.betbrain.sepc.connector.sportsmodel.EventInfoType;
import com.betbrain.sepc.connector.sportsmodel.EventStatus;
import com.betbrain.sepc.connector.sportsmodel.EventType;
import com.betbrain.sepc.connector.sportsmodel.Outcome;
import com.betbrain.sepc.connector.sportsmodel.Sport;

public class QueryTests {
	
	public static void main(String[] args) {
		ModelShortName.initialize();
		DynamoWorker.initialize();
		
		//all sports
		//ArrayList<Entity> sports = new B3KeyEntity(Sport.class).listEntities();
		
		ArrayList<Long> ids;
		//ids = new B3KeyLink(sports.get(0), Event.class).listLinks();
		//B3KeyEntity.load(Event.class, ids);
		
		//all event types
		//new B3KeyEntity(EventType.class).listEntities();
		
		//all event statuses
		//new B3KeyEntity(EventStatus.class).listEntities();

		new B3KeyEntity(EventInfoType.class).listEntities();
		new B3KeyEntity(EventInfo.class).listEntities();
		B3KeyEntity.load(EventInfoType.class, 92);
		
		//event to outcome
		//ids = new B3KeyLink(Event.class, 217409474, Outcome.class).listLinks();
		//B3KeyEntity.load(Outcome.class, ids);
		
		//outcome to offer
		//new B3KeyLink(Outcome.class, 2890125761l, BettingOffer.class).listLinks();
	}
}
