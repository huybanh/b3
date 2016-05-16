package com.betbrain.b3.report.detailedodds;

import java.io.BufferedOutputStream;
import java.io.ByteArrayOutputStream;
import java.io.PrintStream;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.Iterator;
import java.util.LinkedList;

import com.betbrain.b3.data.*;
import com.betbrain.b3.model.*;
import com.betbrain.b3.pushclient.JsonMapper;
import com.betbrain.b3.report.IDs;

public class DetailedOddsTable2 {
	
	private JsonMapper mapper;
	
	//report inputs
	private final long matchId;
	private final long eventPartId;
	
	private ArrayList<RevisionedEntity<B3EventInfo>> statuses;
	private ArrayList<RevisionedEntity<B3EventInfo>> scores;
	private ArrayList<RevisionedEntity<B3BettingOffer>>[] offersWinner;
	private ArrayList<RevisionedEntity<B3BettingOffer>>[] offersDraw;
	
	private ArrayList<B3Outcome> outcomesDraw;
	private ArrayList<B3Outcome> outcomesWinner;
	
	public LinkedList<DetailedOddsTableData> outputData;
	
	public ByteArrayOutputStream outStream;
	
	private PrintStream out;

	public static void main(String[] args) {
		
		DynamoWorker.initBundleCurrent();
		//DynamoWorker.initBundleByStatus("SPRINT2");
		
		/*B3KeyEvent eventKey = new B3KeyEvent(IDs.EVENT_PREMIERLEAGUE, IDs.EVENTTYPE_GENERICMATCH, "20160515");
		@SuppressWarnings("unchecked")
		ArrayList<B3Event> eventIds = (ArrayList<B3Event>) eventKey.listEntities(false, mapper);
		int i = 0;
		for (B3Event e : eventIds) {
			if (i++ != 0) {
				continue;
			}
			System.out.println(e.entity);
			new DetailedOddsTable2(e.entity.getId(), IDs.EVENTPART_ORDINARYTIME).run();
		}*/
		
		JsonMapper jsonMapper = new JsonMapper();
		DetailedOddsTable2 report = new DetailedOddsTable2(
				217633296, IDs.EVENTPART_ORDINARYTIME, false, jsonMapper);
		report.run();
		LinkedList<DetailedOddsTableData> data = report.outputData;
		System.out.println("OUTPUT JSON");
		System.out.println(jsonMapper.deepSerialize(data));
	}
	
	public DetailedOddsTable2(long matchId, long eventPartId, boolean plainText, JsonMapper mapper) {
		this.matchId = matchId;
		this.eventPartId = eventPartId;
		this.mapper = mapper;
		if (plainText) {
			outStream = new ByteArrayOutputStream();
			out = new PrintStream(new BufferedOutputStream(outStream));
		} else {
			outputData = new LinkedList<>();
		}
	}

	public void run() {
		queryData();
		
		for (int i = 0; i < offersWinner.length; i++) {
			DetailedOddsPart part = new DetailedOddsPart("Winner", 
					outcomesWinner.get(i).entity.getParamParticipantId1(),
					statuses, scores, offersWinner[i], out);
			if (outputData != null) {
				outputData.add(part.data);
			}
		}
		for (int i = 0; i < offersDraw.length; i++) {
			DetailedOddsPart part = new DetailedOddsPart("Draw ", 
					//outcomesDraw.get(i).entity.getParamParticipantId1(),
					null, statuses, scores, offersDraw[i], out);
			if (outputData != null) {
				outputData.add(part.data);
			}
		}
		
		if (out != null) {
			out.close();
		}
	}
	
	@SuppressWarnings("unchecked")
	private void queryData() {
		
		long startTime = System.currentTimeMillis();
		
		B3KeyEventInfo statusKey = new B3KeyEventInfo(matchId, eventPartId, IDs.EVENTINFOTYPE_CURRENTSTATUS, null);
		statuses = (ArrayList<RevisionedEntity<B3EventInfo>>) statusKey.listEntities(true, mapper);
		
		B3KeyEventInfo scoreKey = new B3KeyEventInfo(matchId, eventPartId, IDs.EVENTINFOTYPE_SCORE, null);
		scores = (ArrayList<RevisionedEntity<B3EventInfo>>) scoreKey.listEntities(true, mapper);

		
		B3KeyOutcome outcomeKey = new B3KeyOutcome(matchId, eventPartId, IDs.OUTCOMETYPE_DRAW, null);
		outcomesDraw = (ArrayList<B3Outcome>) outcomeKey.listEntities(false, mapper);
		Iterator<B3Outcome> it = outcomesDraw.iterator();
		while (it.hasNext()) {
			B3Outcome o = it.next();
			if (o.entity.getIsNegation()) {
				it.remove();
				continue;
			}
			System.out.println("Got outcome-draw: " + o.entity);
		}
		
		outcomeKey = new B3KeyOutcome(matchId, eventPartId, IDs.OUTCOMETYPE_WINNER, null);
		outcomesWinner = (ArrayList<B3Outcome>) outcomeKey.listEntities(false, mapper);
		it = outcomesWinner.iterator();
		while (it.hasNext()) {
			B3Outcome o = it.next();
			if (o.entity.getIsNegation()) {
				it.remove();
				continue;
			}
			System.out.println("Got outcome-winner: " + o.entity);
		}
		
		offersDraw = new ArrayList[outcomesDraw.size()];
		for (int i = 0; i < outcomesDraw.size(); i++) {
			//System.out.println("outcome: " + outcomesDraw.get(i).entity);
			B3KeyOffer offerKey = new B3KeyOffer(matchId, eventPartId, 
					IDs.OUTCOMETYPE_DRAW, outcomesDraw.get(i).entity.getId(), IDs.BETTINGTYPE_1X2, null);
			offersDraw[i] = (ArrayList<RevisionedEntity<B3BettingOffer>>) offerKey.listEntities(true, mapper);
		}

		offersWinner = new ArrayList[outcomesWinner.size()];
		for (int i = 0; i < outcomesWinner.size(); i++) {
			//System.out.println("outcome: " + outcomesWinner.get(i).entity);
			B3KeyOffer offerKey = new B3KeyOffer(matchId, eventPartId, 
					IDs.OUTCOMETYPE_WINNER, outcomesWinner.get(i).entity.getId(), IDs.BETTINGTYPE_1X2, null);
			offersWinner[i] = (ArrayList<RevisionedEntity<B3BettingOffer>>) offerKey.listEntities(true, mapper);
		}
		
		System.out.println("Data querying time: " + (System.currentTimeMillis() - startTime));
		
		print("Match statuses", statuses);
		print("Scores", scores);
		//print("Offers: winner1", offersWinner1);
		//print("Offers: winner2", offersWinner2);
		//print("Offers: draw", offersDraw);
		
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
