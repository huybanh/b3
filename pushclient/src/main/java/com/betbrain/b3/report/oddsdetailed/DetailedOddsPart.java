package com.betbrain.b3.report.oddsdetailed;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Date;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.Map.Entry;
import java.util.TreeMap;

import com.betbrain.b3.data.RevisionedEntity;
import com.betbrain.b3.model.B3BettingOffer;
import com.betbrain.b3.model.B3Entity;
import com.betbrain.b3.model.B3EventInfo;

class DetailedOddsPart {
	
	/*private ArrayList<RevisionedEntity<B3EventInfo>> statuses;
	private ArrayList<RevisionedEntity<B3EventInfo>> scores;
	private ArrayList<RevisionedEntity<B3BettingOffer>> offers;*/
	
	//private TreeMap<Long, DetailedOddsItemStatus> statuses = new TreeMap<>();
	//private TreeMap<Long, DetailedOddsItemScore> scores = new TreeMap<>();
	//private TreeMap<Long, DetailedOddsItemOffer> odds = new TreeMap<>();
	
	private LinkedList<Long> timePoints = new LinkedList<>();
	private String strDetails = "";

	public DetailedOddsPart(String caption, ArrayList<RevisionedEntity<B3EventInfo>> statusList,
			ArrayList<RevisionedEntity<B3EventInfo>> scoreList, ArrayList<RevisionedEntity<B3BettingOffer>> offerList) {
		
		/*this.statuses = statuses;
		this.scores = scores;
		this.offers = offers;*/
		
		HashSet<Long> timeset = new HashSet<>();
		DetailedOddsColumnSet statuses = new DetailedOddsColumnSet();
		statuses.typeName = "Match status";
		for (RevisionedEntity<B3EventInfo> one : statusList) {
			statuses.add(new DetailedOddsItemStatus(one));
			timeset.add(one.time);
		}

		DetailedOddsColumnSet scores = new DetailedOddsColumnSet();
		statuses.typeName = "Score";
		for (RevisionedEntity<B3EventInfo> one : scoreList) {
			scores.add(new DetailedOddsItemScore(one));
			timeset.add(one.time);
		}

		DetailedOddsColumnSet odds = new DetailedOddsColumnSet();
		statuses.typeName = "Odds";
		for (RevisionedEntity<B3BettingOffer> one : offerList) {
			odds.add(new DetailedOddsItemOffer(one));
			timeset.add(one.time);
		}
		
		timePoints.addAll(timeset);
		Collections.sort(timePoints);
		
		odds.prepare();
		scores.prepare();
		statuses.prepare();
		StringBuilder sb = new StringBuilder();
		System.out.println(caption);
		sb.append(caption + "\n");
		System.out.print("Time | ");
		sb.append("Time | ");
		for (String name : odds.providerNames) {
			System.out.print("Odds from " + name + " | ");
			sb.append("Odds from " + name + " | ");
		}
		for (String name : scores.providerNames) {
			System.out.print("Score from " + name + " | ");
			sb.append("Score from " + name + " | ");
		}
		for (String name : statuses.providerNames) {
			System.out.print("Match status from " + name + " | ");
			sb.append("Match status from " + name + " | ");
		}
		System.out.println();
		sb.append("\n");
		String partReport;
		for (Long time : timePoints) {
			if (time == 0) {
				continue;
			}
			System.out.print(time + ": " + new Date(time) + " | ");
			sb.append(time + ": " + new Date(time) + " | ");
			
			for (int i = 0; i < odds.providerNames.length; i++) {
				partReport = odds.getValue(time, i++) + " | ";
				System.out.print(partReport);
				sb.append(partReport);
			}
			for (int i = 0; i < scores.providerNames.length; i++) {
				partReport = scores.getValue(time, i++) + " | ";
				System.out.print(partReport);
				sb.append(partReport);
			}
			for (int i = 0; i < statuses.providerNames.length; i++) {
				partReport = statuses.getValue(time, i++) + " | ";
				System.out.print(partReport);
				sb.append(partReport);
			}
			System.out.println();
			sb.append("\n");
		}
		setStrDetails(sb.toString());
	}

	public String getStrDetails() {
		return strDetails;
	}

	public void setStrDetails(String strDetails) {
		this.strDetails = strDetails;
	}

}

class DetailedOddsColumnSet {
	
	String typeName;
	private HashMap<String, TreeMap<Long, DetailedOddsItem<?>>> providerMap = new HashMap<>();
	
	String[] providerNames;
	private TreeMap<Long, DetailedOddsItem<?>>[] itemsByProvider;
	
	private DetailedOddsItem<?> lastItem;
	
	void add(DetailedOddsItem<?> item) {
		TreeMap<Long, DetailedOddsItem<?>> subMap = providerMap.get(item.getProvider());
		if (subMap == null) {
			subMap = new TreeMap<>();
			providerMap.put(item.getProvider(), subMap);
		}
		subMap.put(item.getTime(), item);
	}
	
	@SuppressWarnings("unchecked")
	void prepare() {
		providerNames = new String[providerMap.size()];
		itemsByProvider = new TreeMap[providerMap.size()];
		int index = 0;
		for (Entry<String, TreeMap<Long, DetailedOddsItem<?>>> entry : providerMap.entrySet()) {
			providerNames[index] = entry.getKey();
			itemsByProvider[index] = entry.getValue();
			index++;
		}
	}
	
	String getValue(long time, int index) {
		DetailedOddsItem<?> item = itemsByProvider[index].get(time);
		if (item == null) {
			if (lastItem == null) {
				return "   ";
			} else {
				//return lastItem.getValue();
				return "(" + lastItem.getValue() + ")";
			}
		}
		lastItem = item;
		return item.getValue();
	}
}

abstract class DetailedOddsItem<E extends B3Entity<?>> {
	
	RevisionedEntity<E> revisionedEntity;
	
	public DetailedOddsItem(RevisionedEntity<E> revisionedEntity) {
		super();
		this.revisionedEntity = revisionedEntity;
	}
	
	long getTime() {
		return revisionedEntity.time;
	}
	
	abstract String getProvider();
	
	abstract String getValue();
}

class DetailedOddsItemStatus extends DetailedOddsItem<B3EventInfo> {
	
	private static String[] CAPTIONS = {"Pending", "In Progress", "Ended"};

	public DetailedOddsItemStatus(RevisionedEntity<B3EventInfo> revisionedEntity) {
		super(revisionedEntity);
	}

	@Override
	String getProvider() {
		if (revisionedEntity.b3entity.provider == null) {
			return revisionedEntity.b3entity.entity.getProviderId() + "";
		}
		return revisionedEntity.b3entity.provider.entity.getName();
	}

	@Override
	String getValue() {
		long id = (Long) this.revisionedEntity.b3entity.entity.getParamEventStatusId1() - 1;
		return CAPTIONS[(int) id];
	}
}

class DetailedOddsItemScore extends DetailedOddsItem<B3EventInfo> {

	public DetailedOddsItemScore(RevisionedEntity<B3EventInfo> revisionedEntity) {
		super(revisionedEntity);
	}

	@Override
	String getProvider() {
		if (revisionedEntity.b3entity.provider == null) {
			return revisionedEntity.b3entity.entity.getProviderId() + "";
		}
		return revisionedEntity.b3entity.provider.entity.getName();
	}

	@Override
	String getValue() {
		int i1 = this.revisionedEntity.b3entity.entity.getParamFloat1().intValue();
		int i2 = this.revisionedEntity.b3entity.entity.getParamFloat2().intValue();
		return i1 + ":" + i2;
	}
	
}

class DetailedOddsItemOffer extends DetailedOddsItem<B3BettingOffer> {

	public DetailedOddsItemOffer(RevisionedEntity<B3BettingOffer> revisionedEntity) {
		super(revisionedEntity);
	}

	@Override
	String getProvider() {
		if (this.revisionedEntity.b3entity.provider == null) {
			return revisionedEntity.b3entity.entity.getProviderId() + "";
		}
		return this.revisionedEntity.b3entity.provider.entity.getName();
	}

	@Override
	String getValue() {
		return revisionedEntity.b3entity.entity.getOdds() + "";
	}
	
}