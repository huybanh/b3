package com.betbrain.b3.report.oddsdetailed;

import java.util.ArrayList;

import com.betbrain.b3.data.*;
import com.betbrain.b3.pushclient.JsonMapper;
import com.betbrain.b3.report.IDs;
import com.betbrain.sepc.connector.sportsmodel.*;

public class DetailedOddsTable {
	
	private JsonMapper mapper = new JsonMapper();
	
	private long matchId = 217562668L;
	private long outcomeIdWinner1 = 2954860246L;
	private long outcomeIdWinner2 = 2954860247L;
	private long outcomeIdDraw = 2954860248L;
	
	private long sportId = IDs.SPORT_FOOTBALL;
	private long eventTypeId = IDs.EVENTTYPE_GENERICMATCH;
	
	private ArrayList<EventInfo> statuses;
	private ArrayList<EventInfo> scores;
	private ArrayList<BettingOffer> offersWinner1;
	private ArrayList<BettingOffer> offersWinner2;
	private ArrayList<BettingOffer> offersDraw;

	public static void main(String[] args) {
		
		//DynamoWorker.initBundleCurrent();
		DynamoWorker.initBundleByStatus("SPRINT2");
		
		new DetailedOddsTable().run();
		//new DetailedOddsTable().run();
	}

	public void run() {
		queryData();
	}
	
	private void queryData() {
		
		long startTime = System.currentTimeMillis();
		B3KeyEventInfo statusKey = new B3KeyEventInfo(217562668L, IDs.EVENTINFOTYPE_CURRENTSTATUS, null);
		statuses = statusKey.listEntities(true, mapper);
		
		B3KeyEventInfo scoreKey = new B3KeyEventInfo(matchId, IDs.EVENTINFOTYPE_SCORE, null);
		scores = scoreKey.listEntities(true, mapper);
		
		B3KeyOffer offerKey = new B3KeyOffer(sportId, eventTypeId, matchId, 
				IDs.OUTCOME_WINNER, outcomeIdWinner1, IDs.BETTINGTYPE_1X2, null);
		offersWinner1 = offerKey.listEntities(true, mapper);
		
		offerKey = new B3KeyOffer(sportId, eventTypeId, matchId, 
				IDs.OUTCOME_WINNER, outcomeIdWinner2, IDs.BETTINGTYPE_1X2, null);
		offersWinner2 = offerKey.listEntities(true, mapper);
		
		offerKey = new B3KeyOffer(sportId, eventTypeId, matchId, 
				IDs.OUTCOME_DRAW, outcomeIdDraw, IDs.BETTINGTYPE_1X2, null);
		offersDraw = offerKey.listEntities(true, mapper);
		System.out.println("Data querying time: " + (System.currentTimeMillis() - startTime));
		
		print("Match statuses", statuses);
		print("Scores", scores);
		print("Offers: winner1", offersWinner1);
		print("Offers: winner2", offersWinner2);
		print("Offers: draw", offersDraw);
	}
	
	private static void print(String caption, ArrayList<?> data) {
		System.out.println(caption);
		for (Object one : data) {
			System.out.println(one);
		}
	}
}
