package com.betbrain.b3.report.oddsdetailed;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.Iterator;

import com.betbrain.b3.data.*;
import com.betbrain.b3.model.B3BettingOffer;
import com.betbrain.b3.model.B3Entity;
import com.betbrain.b3.model.B3EventInfo;
import com.betbrain.b3.pushclient.JsonMapper;
import com.betbrain.b3.report.IDs;

import flexjson.JSONSerializer;

import com.betbrain.b3.model.DetailsOddEntity;

public class DetailedOddsTable {
	
	private JsonMapper mapper = new JsonMapper();
	
	private long matchId = 217562668L;
	private long outcomeIdWinner1 = 2954860246L;
	private long outcomeIdWinner2 = 2954860247L;
	private long outcomeIdDraw = 2954860248L;
	
	private long sportId = IDs.SPORT_FOOTBALL;
	private long eventTypeId = IDs.EVENTTYPE_GENERICMATCH;
	
	private ArrayList<RevisionedEntity<B3EventInfo>> statuses;
	private ArrayList<RevisionedEntity<B3EventInfo>> scores;
	private ArrayList<RevisionedEntity<B3BettingOffer>> offersWinner1;
	private ArrayList<RevisionedEntity<B3BettingOffer>> offersWinner2;
	private ArrayList<RevisionedEntity<B3BettingOffer>> offersDraw;

	public static void main(String[] args) {
		
		//DynamoWorker.initBundleCurrent();
		DynamoWorker.initBundleByStatus("SPRINT2");
		String matchId = "217562668";
		String reportDetails = new DetailedOddsTable(Long.parseLong(matchId)).run();
		System.out.println(reportDetails);
	}

	//Running with default matchid
	public DetailedOddsTable()
	{
	}

	public DetailedOddsTable(long matchId)
	{
		this.matchId = matchId;
	}
	public String run() {
		queryData();
		DetailsOddEntity entity = new DetailsOddEntity();
		entity.getDataReport().add(new DetailedOddsPart("Detailed 1x2 odds: Winner1", statuses, scores, offersWinner1).getOddsPart());		
		entity.getDataReport().add(new DetailedOddsPart("Detailed 1x2 odds: Winner2", statuses, scores, offersWinner2).getOddsPart());
		entity.getDataReport().add(new DetailedOddsPart("Detailed 1x2 odds: Draw", statuses, scores, offersDraw).getOddsPart());
		JSONSerializer flexSer = new JSONSerializer();
		return flexSer.exclude("*.class").deepSerialize(entity);
	}
	
	@SuppressWarnings("unchecked")
	private void queryData() {
		
		long startTime = System.currentTimeMillis();
		//Get All EventInfo from match to get status and score
		B3KeyEventInfo statusKey = new B3KeyEventInfo(matchId, IDs.EVENTINFOTYPE_CURRENTSTATUS, null);
		statuses = (ArrayList<RevisionedEntity<B3EventInfo>>) statusKey.listEntities(true, mapper);
		
		B3KeyEventInfo scoreKey = new B3KeyEventInfo(matchId, IDs.EVENTINFOTYPE_SCORE, null);
		scores = (ArrayList<RevisionedEntity<B3EventInfo>>) scoreKey.listEntities(true, mapper);
		
		//Get all outcome from current match
		
		//Get all betting offer with all outcome id
		B3KeyOffer offerKey = new B3KeyOffer(sportId, eventTypeId, matchId, 
				IDs.OUTCOME_WINNER, outcomeIdWinner1, IDs.BETTINGTYPE_1X2, null);
		offersWinner1 = (ArrayList<RevisionedEntity<B3BettingOffer>>) offerKey.listEntities(true, mapper);
		
		offerKey = new B3KeyOffer(sportId, eventTypeId, matchId, 
				IDs.OUTCOME_WINNER, outcomeIdWinner2, IDs.BETTINGTYPE_1X2, null);
		offersWinner2 = (ArrayList<RevisionedEntity<B3BettingOffer>>) offerKey.listEntities(true, mapper);
		
		offerKey = new B3KeyOffer(sportId, eventTypeId, matchId, 
				IDs.OUTCOME_DRAW, outcomeIdDraw, IDs.BETTINGTYPE_1X2, null);
		offersDraw = (ArrayList<RevisionedEntity<B3BettingOffer>>) offerKey.listEntities(true, mapper);
		System.out.println("Data querying time: " + (System.currentTimeMillis() - startTime));
		
		print("Match statuses", statuses);
		print("Scores", scores);
		print("Offers: winner1", offersWinner1);
		print("Offers: winner2", offersWinner2);
		print("Offers: draw", offersDraw);
		
		//TODO remove this hack
		Iterator<RevisionedEntity<B3EventInfo>> it = scores.iterator();
		while (it.hasNext()) {
			RevisionedEntity<B3EventInfo> i = it.next();
			if (i.b3entity.entity.getEventPartId() != IDs.EVENTPART_ORDINARYTIME) {
				it.remove();
			}
		}
	}
	
	@SuppressWarnings("unchecked")
	private static void print(String caption, @SuppressWarnings("rawtypes") ArrayList data) {
		Collections.sort(data, new Comparator<RevisionedEntity<?>>() {

			@Override
			public int compare(RevisionedEntity<?> o1, RevisionedEntity<?> o2) {
				return (int) (o1.time - o2.time);
			}
		});
		System.out.println(caption);
		for (Object obj : data) {
			RevisionedEntity<?> one = (RevisionedEntity<?>) obj;
			Object provider = null;
			if (one.b3entity instanceof B3EventInfo) {
				if (((B3EventInfo) one.b3entity).provider != null) {
					provider = ((B3EventInfo) one.b3entity).provider.entity;
				}
			}
			System.out.println(one.time + "-" + ((B3Entity<?>) one.b3entity).entity + " / " + provider);
		}
	}
}
