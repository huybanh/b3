package com.betbrain.b3.report;

import java.util.ArrayList;

import com.betbrain.b3.data.B3KeyEntity;
import com.betbrain.b3.data.B3KeyLink;
import com.betbrain.b3.data.DynamoWorker;
import com.betbrain.b3.pushclient.JsonMapper;
import com.betbrain.sepc.connector.sportsmodel.*;

public class EventQuery {

	private static JsonMapper jsonMapper = new JsonMapper();
	
	public static void main(String[] args) {
		DynamoWorker.initBundleCurrent();

		//query(219387861);
		//query(219501132);
		query(IDs.EVENT_PREMIERLEAGUE);
	}
	
	private static void query(long leagueId) {
		
		//league
		B3KeyEntity keyEntity = new B3KeyEntity(Event.class, leagueId);
		Event league = keyEntity.load(jsonMapper);
		System.out.println("League: " + league);
		
		//matches
		B3KeyLink keyLink = new B3KeyLink(league, Event.class, Event.PROPERTY_NAME_parentId);
		ArrayList<Long> matchIds = keyLink.listLinks();
		System.out.println(matchIds);
		System.out.println("Search for matches");
		for (long matchId : matchIds) {
			match(matchId);
		}
		
		/*for (Long oneId : matchIds) {
			keyEntity = new B3KeyEntity(Event.class, oneId);
			Event match = keyEntity.load(jsonMapper);
			System.out.println(match);
			
			keyLink = new B3KeyLink(match, Outcome.class, Outcome.PROPERTY_NAME_eventId);
			matchIds = keyLink.listLinks();
			System.out.println(matchIds);
		}*/

		
	}
	
	private static void match(long matchId) {
		//match
		//B3KeyEntity keyEntity = new B3KeyEntity(Event.class, matchId);
		//Event match = keyEntity.load(jsonMapper);
		//System.out.println("Match: " + match);
		
		//outcomes
		B3KeyLink keyLink = new B3KeyLink(Event.class, matchId, Outcome.class, Outcome.PROPERTY_NAME_eventId);
		ArrayList<Long> outcomeIds = keyLink.listLinks();
		System.out.println("Outcome count: " + outcomeIds.size());
	}
	
	@SuppressWarnings("unused")
	private static void offer() {
		
		//bettingoffer
		/*keyLink = new B3KeyLink(Outcome.class, outcomeIds.get(0), BettingOffer.class, BettingOffer.PROPERTY_NAME_outcomeId);
		ArrayList<Long> offerIds = keyLink.listLinks();
		System.out.println(offerIds.size());
		//B3KeyEntity.load(jsonMapper, BettingOffer.class, offerIds);
		
		//eventinfo - current status
		keyLink = new B3KeyLink(Event.class, matchIds.get(0), EventInfo.class, "eventId");
		ArrayList<Long> infoIds = keyLink.listLinks();
		//B3KeyEntity.load(jsonMapper, EventInfo.class, infoIds);
		System.out.println(infoIds.size());*/
	}
}
