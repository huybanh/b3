package com.betbrain.b3.report;

import java.util.ArrayList;

import com.betbrain.b3.data.B3KeyEntity;
import com.betbrain.b3.data.B3KeyLink;
import com.betbrain.b3.data.DynamoWorker;
import com.betbrain.b3.data.ModelShortName;
import com.betbrain.b3.pushclient.JsonMapper;
import com.betbrain.sepc.connector.sportsmodel.BettingOffer;
import com.betbrain.sepc.connector.sportsmodel.Event;
import com.betbrain.sepc.connector.sportsmodel.EventInfo;
import com.betbrain.sepc.connector.sportsmodel.Outcome;

public class EventQuery {
	
	public static void main(String[] args) {
		ModelShortName.initialize();
		DynamoWorker.initBundleCurrent();

		//query(219387861);
		//query(219501132);
		query(206795928);
	}
	
	private static void query(long eventId) {

		JsonMapper jsonMapper = new JsonMapper();
		
		//event
		B3KeyEntity keyEntity = new B3KeyEntity(Event.class, eventId);
		Event event = keyEntity.load(jsonMapper);
		
		//outcomes
		B3KeyLink keyLink = new B3KeyLink(Event.class, eventId, Outcome.class, "eventId");
		ArrayList<Long> outcomeIds = keyLink.listLinks();
		B3KeyEntity.load(jsonMapper, Outcome.class, outcomeIds);
		
		//bettingoffer
		keyLink = new B3KeyLink(Outcome.class, outcomeIds.get(0), BettingOffer.class, "outcomeId");
		ArrayList<Long> offerIds = keyLink.listLinks();
		B3KeyEntity.load(jsonMapper, BettingOffer.class, offerIds);
		
		//eventinfo - current status
		keyLink = new B3KeyLink(Event.class, eventId, EventInfo.class, "eventId");
		ArrayList<Long> infoIds = keyLink.listLinks();
		B3KeyEntity.load(jsonMapper, EventInfo.class, infoIds);
	}
}
