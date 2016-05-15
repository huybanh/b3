package com.betbrain.b3.report.oddsdetailed;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;

import com.betbrain.b3.data.*;
import com.betbrain.b3.model.B3BettingOffer;
import com.betbrain.b3.model.B3Entity;
import com.betbrain.b3.model.B3EventInfo;
import com.betbrain.b3.model.B3Outcome;
import com.betbrain.b3.pushclient.JsonMapper;
import com.betbrain.b3.report.IDs;

@Deprecated
public class DetailedOddsTable3 {
	
	private static JsonMapper mapper = new JsonMapper();
	
	//report inputs
	private final long matchId;
	private final long eventPartId;
	
	private ArrayList<RevisionedEntity<B3EventInfo>> statuses;
	private ArrayList<RevisionedEntity<B3EventInfo>> scores;

	private ArrayList<RevisionedEntity<B3BettingOffer>> offersWinner1;
	private ArrayList<RevisionedEntity<B3BettingOffer>> offersWinner2;
	private ArrayList<RevisionedEntity<B3BettingOffer>> offersDraw;

	public static void main(String[] args) {
		
		DynamoWorker.initBundleCurrent();
		//DynamoWorker.initBundleByStatus("SPRINT2");
		
		new DetailedOddsTable2(217633293, IDs.EVENTPART_ORDINARYTIME).run();
	}
	
	public DetailedOddsTable3(long matchId, long eventPartId) {
		this.matchId = matchId;
		this.eventPartId = eventPartId;
	}

	public void run() {
		queryData();
		new DetailedOddsPart("Detailed 1x2 odds: Winner 1", statuses, scores, offersWinner1);
		new DetailedOddsPart("Detailed 1x2 odds: Winner 2", statuses, scores, offersWinner2);
		new DetailedOddsPart("Detailed 1x2 odds: Draw ", statuses, scores, offersDraw);
	}
	
	@SuppressWarnings("unchecked")
	private void queryData() {
		
		long startTime = System.currentTimeMillis();
		
		B3KeyEventInfo statusKey = new B3KeyEventInfo(217562668L, eventPartId, IDs.EVENTINFOTYPE_CURRENTSTATUS, null);
		statuses = (ArrayList<RevisionedEntity<B3EventInfo>>) statusKey.listEntities(true, mapper);
		
		B3KeyEventInfo scoreKey = new B3KeyEventInfo(matchId, eventPartId, IDs.EVENTINFOTYPE_SCORE, null);
		scores = (ArrayList<RevisionedEntity<B3EventInfo>>) scoreKey.listEntities(true, mapper);

		
		B3KeyOutcome outcomeKey = new B3KeyOutcome(matchId, eventPartId, IDs.OUTCOMETYPE_DRAW, null);
		B3Outcome outcomesDraw = (B3Outcome) outcomeKey.listEntities(false, mapper).get(0);
		
		outcomeKey = new B3KeyOutcome(matchId, eventPartId, IDs.OUTCOMETYPE_WINNER, null);
		B3Outcome outcomeWinner1 = (B3Outcome) outcomeKey.listEntities(false, mapper).get(0);
		B3Outcome outcomeWinner2 = (B3Outcome) outcomeKey.listEntities(false, mapper).get(1);
		
		B3KeyOffer offerKey = new B3KeyOffer(matchId, eventPartId, 
				IDs.OUTCOMETYPE_DRAW, outcomesDraw.entity.getId(), IDs.BETTINGTYPE_1X2, null);
		offersDraw = (ArrayList<RevisionedEntity<B3BettingOffer>>) offerKey.listEntities(true, mapper);

		offerKey = new B3KeyOffer(matchId, eventPartId, 
				IDs.OUTCOMETYPE_WINNER, outcomeWinner1.entity.getId(), IDs.BETTINGTYPE_1X2, null);
		offersWinner1 = (ArrayList<RevisionedEntity<B3BettingOffer>>) offerKey.listEntities(true, mapper);
		
		offerKey = new B3KeyOffer(matchId, eventPartId, 
				IDs.OUTCOMETYPE_WINNER, outcomeWinner2.entity.getId(), IDs.BETTINGTYPE_1X2, null);
		offersWinner2 = (ArrayList<RevisionedEntity<B3BettingOffer>>) offerKey.listEntities(true, mapper);
		
		System.out.println("Data querying time: " + (System.currentTimeMillis() - startTime));
		
		print("Match statuses", statuses);
		print("Scores", scores);
		print("Offers: winner1", offersWinner1);
		print("Offers: winner2", offersWinner2);
		print("Offers: draw", offersDraw);
		
		/*Iterator<RevisionedEntity<B3EventInfo>> it = scores.iterator();
		while (it.hasNext()) {
			RevisionedEntity<B3EventInfo> i = it.next();
			if (i.b3entity.entity.getEventPartId() != IDs.EVENTPART_ORDINARYTIME) {
				it.remove();
			}
		}*/
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
