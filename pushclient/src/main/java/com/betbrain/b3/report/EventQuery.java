package com.betbrain.b3.report;

import java.util.ArrayList;

import com.betbrain.b3.data.*;
import com.betbrain.b3.model.B3EventInfo;
import com.betbrain.b3.model.B3Outcome;
import com.betbrain.b3.pushclient.JsonMapper;
import com.betbrain.sepc.connector.sportsmodel.*;

public class EventQuery {

	private static JsonMapper jsonMapper = new JsonMapper();
	
	public static void main(String[] args) {
		DynamoWorker.initBundleCurrent();

		//query(219387861);
		//query(219501132);
		//query(IDs.EVENT_PREMIERLEAGUE);
		match(219900664, 13, 14);
	}
	
	private static void match(long matchId, long... outcomeTypeIds) {
		//match
		B3KeyEntity keyEntity = new B3KeyEntity(Event.class, matchId);
		Event match = keyEntity.load(jsonMapper);
		System.out.println("Match: " + match);
		
		//outcomes
		/*B3KeyLink keyLink = new B3KeyLink(Event.class, matchId, Outcome.class, Outcome.PROPERTY_NAME_eventId);
		ArrayList<Long> outcomeIds = keyLink.listLinks();
		System.out.println("Outcome count: " + outcomeIds.size());*/
		
		B3KeyOutcome outcomeKey = new B3KeyOutcome(matchId, null, null, null);
		ArrayList<?> outcomes = outcomeKey.listEntities(false, jsonMapper);
		int i = 0;
		for (Object o : outcomes) {
			Outcome oc = ((B3Outcome) o).entity;
			for (long l : outcomeTypeIds) {
				if (oc.getTypeId() == l) {
					System.out.println(++i + ": " + oc);
				}
			}
		}
		
		//offer(outcomeIds.get(0));
		info(matchId);
	}
	
	/*private static void offer(long outcomeId) {
		
		//bettingoffer
		B3KeyLink keyLink = new B3KeyLink(Outcome.class, outcomeId, BettingOffer.class, BettingOffer.PROPERTY_NAME_outcomeId);
		ArrayList<Long> offerIds = keyLink.listLinks();
		System.out.println("Offer count: " + offerIds.size());
	}*/
	
	private static void info(long matchId) {
		
		//eventinfo - current status
		/*B3KeyLink keyLink = new B3KeyLink(Event.class, matchId, EventInfo.class, "eventId");
		ArrayList<Long> infoIds = keyLink.listLinks();
		//B3KeyEntity.load(jsonMapper, EventInfo.class, infoIds);
		System.out.println("Info count: " + infoIds.size());*/
		
		B3KeyEventInfo infoKey = new B3KeyEventInfo(matchId);
		ArrayList<?> infos = infoKey.listEntities(false, jsonMapper);
		int i = 0;
		for (Object o : infos) {
			EventInfo ei = ((B3EventInfo) o).entity;
			if (ei.getTypeId() == 1 || ei.getTypeId() == 92) {
				System.out.println(++i + ": " + ei);
			}
		}
	}
}
