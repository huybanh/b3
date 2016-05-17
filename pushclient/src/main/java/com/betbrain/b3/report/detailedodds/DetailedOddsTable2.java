package com.betbrain.b3.report.detailedodds;

import java.io.BufferedOutputStream;
import java.io.ByteArrayOutputStream;
import java.io.PrintStream;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.Iterator;
import java.util.LinkedList;

import com.betbrain.b3.api.B3Engine;
import com.betbrain.b3.data.*;
import com.betbrain.b3.model.*;
import com.betbrain.b3.pushclient.JsonMapper;
import com.betbrain.b3.report.IDs;

public class DetailedOddsTable2 {
	
	private final B3Engine b3;
	
	//report inputs
	private final long matchId;
	private final long eventPartId;
	private final long bettingType;
	
	//report params
	private final Float paramFloat1;
	private final Float paramFloat2; 
	private final Float paramFloat3;
	private final Boolean paramBoolean1; 
	private final String paramString1;
	
	private ArrayList<RevisionedEntity<B3EventInfo>> statuses;
	private ArrayList<RevisionedEntity<B3EventInfo>> scores;
	//private ArrayList<RevisionedEntity<B3BettingOffer>>[] offersWinner;
	//private ArrayList<RevisionedEntity<B3BettingOffer>>[] offersDraw;
	private ArrayList<RevisionedEntity<B3BettingOffer>>[] offers;
	
	//private ArrayList<B3Outcome> outcomesDraw;
	//private ArrayList<B3Outcome> outcomesWinner;
	private ArrayList<B3Outcome> outcomes;
	
	public LinkedList<DetailedOddsTableData> outputData;
	
	public ByteArrayOutputStream outStream;
	
	private PrintStream out;

	public static void main(String[] args) {
		
		//DynamoWorker.initBundleCurrent();
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
		
		//JsonMapper jsonMapper = new JsonMapper();
		DetailedOddsTable2 report = new DetailedOddsTable2(new B3Engine(),
				219900664, IDs.EVENTPART_ORDINARYTIME, IDs.BETTINGTYPE_1X2,
				null, null, null, null, null);
		report.setPlainText(true);
		report.run();
		//LinkedList<DetailedOddsTableData> data = report.outputData;
		System.out.println("REPORT OUTPUT");
		//System.out.println(jsonMapper.deepSerialize(report.outputData));
		System.out.println(new String(report.outStream.toByteArray()));
	}
	
	public DetailedOddsTable2(B3Engine b3, long matchId, long eventPartId, long bettingType,
			Float paramFloat1, Float paramFloat2, Float paramFloat3, Boolean paramBoolean1, String paramString1) {
		
		this.b3 = b3;
		this.matchId = matchId;
		this.eventPartId = eventPartId;
		this.bettingType = bettingType;
		
		this.paramFloat1 = paramFloat1;
		this.paramFloat2 = paramFloat2;
		this.paramFloat3 = paramFloat3;
		this.paramBoolean1 = paramBoolean1;
		this.paramString1 = paramString1;
		//this.mapper = mapper;
		outputData = new LinkedList<>();
	}
	
	public void setPlainText(boolean plainText) {
		if (plainText) {
			outStream = new ByteArrayOutputStream();
			out = new PrintStream(new BufferedOutputStream(outStream));
			outputData = null;
		} else {
			outputData = new LinkedList<>();
			outStream = null;
			out = null;
		}
	}

	public void run() {
		queryData();
		
		if (!outcomes.isEmpty()) {
			for (int i = 0; i < outcomes.size(); i++) {
				if (offers[i].isEmpty()) {
					continue;
				}
				DetailedOddsPart part = new DetailedOddsPart(/*"Winner", */
						outcomes.get(i),
						statuses, scores, offers[i], out);
				if (outputData != null) {
					outputData.add(part.data);
				}
			}
			/*for (int i = 0; i < offersDraw.length; i++) {
				DetailedOddsPart part = new DetailedOddsPart("Draw ", 
						//outcomesDraw.get(i).entity.getParamParticipantId1(),
						null, statuses, scores, offersDraw[i], out);
				if (outputData != null) {
					outputData.add(part.data);
				}
			}*/
		} else {
			DetailedOddsPart part = new DetailedOddsPart(/*"Statuses & scores ",
					null,*/ null, statuses, scores, null, out);
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
		JsonMapper mapper = new JsonMapper();
		B3KeyEventInfo statusKey = new B3KeyEventInfo(matchId, eventPartId, IDs.EVENTINFOTYPE_CURRENTSTATUS, null);
		statuses = (ArrayList<RevisionedEntity<B3EventInfo>>) statusKey.listEntities(true, mapper);
		
		B3KeyEventInfo scoreKey = new B3KeyEventInfo(matchId, eventPartId, IDs.EVENTINFOTYPE_SCORE, null);
		scores = (ArrayList<RevisionedEntity<B3EventInfo>>) scoreKey.listEntities(true, mapper);

		outcomes = new ArrayList<>();
		Long[] outcomeTypes = b3.getOutcomeTypeIds(bettingType);
		for (Long oneOutcomeType : outcomeTypes) {
			B3KeyOutcome outcomeKey = new B3KeyOutcome(matchId, eventPartId, oneOutcomeType, null);
			ArrayList<B3Outcome> subList = (ArrayList<B3Outcome>) outcomeKey.listEntities(false, mapper);
			Iterator<B3Outcome> it = subList.iterator();
			while (it.hasNext()) {
				B3Outcome o = it.next();
				if (o.entity.getIsNegation() ||
						(paramFloat1 != null && !paramFloat1.equals(o.entity.getParamFloat1())) ||
						(paramFloat2 != null && !paramFloat1.equals(o.entity.getParamFloat2())) ||
						(paramFloat3 != null && !paramFloat1.equals(o.entity.getParamFloat3())) ||
						(paramBoolean1 != null && !paramFloat1.equals(o.entity.getParamBoolean1())) ||
						(paramString1 != null && !paramFloat1.equals(o.entity.getParamString1()))) {
					it.remove();
					continue;
				}
				//System.out.println("Got outcome: " + o.entity);
			}
			outcomes.addAll(subList);
		}
		
		/*outcomeKey = new B3KeyOutcome(matchId, eventPartId, IDs.OUTCOMETYPE_WINNER, null);
		outcomesWinner = (ArrayList<B3Outcome>) outcomeKey.listEntities(false, mapper);
		it = outcomesWinner.iterator();
		while (it.hasNext()) {
			B3Outcome o = it.next();
			if (o.entity.getIsNegation()) {
				it.remove();
				continue;
			}
			System.out.println("Got outcome-winner: " + o.entity);
		}*/
		
		offers = new ArrayList[outcomes.size()];
		for (int i = 0; i < outcomes.size(); i++) {
			//System.out.println("outcome: " + outcomesDraw.get(i).entity);
			B3KeyOffer offerKey = new B3KeyOffer(matchId, eventPartId, 
					IDs.OUTCOMETYPE_DRAW, outcomes.get(i).entity.getId(), bettingType, null);
			offers[i] = (ArrayList<RevisionedEntity<B3BettingOffer>>) offerKey.listEntities(true, mapper);
		}
		
		/*for (ArrayList<RevisionedEntity<B3BettingOffer>> x : offers) {
			Iterator<RevisionedEntity<B3BettingOffer>> it2 = x.iterator();
			while (it2.hasNext()) {
				RevisionedEntity<B3BettingOffer> y = it2.next();
				if (y.b3entity.entity.getBettingTypeId() != 69) {
					it2.remove();
				}
				System.out.println(y.b3entity.entity);
			}
		}*/

		/*offersWinner = new ArrayList[outcomesWinner.size()];
		for (int i = 0; i < outcomesWinner.size(); i++) {
			//System.out.println("outcome: " + outcomesWinner.get(i).entity);
			B3KeyOffer offerKey = new B3KeyOffer(matchId, eventPartId, 
					IDs.OUTCOMETYPE_WINNER, outcomesWinner.get(i).entity.getId(), bettingType, null);
			offersWinner[i] = (ArrayList<RevisionedEntity<B3BettingOffer>>) offerKey.listEntities(true, mapper);
		}*/
		
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
