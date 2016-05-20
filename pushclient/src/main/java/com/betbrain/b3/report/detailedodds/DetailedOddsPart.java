package com.betbrain.b3.report.detailedodds;

import java.io.PrintStream;
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
import com.betbrain.b3.model.B3Outcome;
import com.betbrain.sepc.connector.sportsmodel.Provider;

class DetailedOddsPart {
	
	private LinkedList<Long> timePoints = new LinkedList<>();
	
	DetailedOddsTableData data;
	
	private static String append(String s, String name, Object value) {
		if (value == null) {
			return s;
		}
		if (s == null) {
			return name + ":" + value;
		} else {
			return s + ", " + name + ":" + value;
		}
	}

	public DetailedOddsPart(/*String outcomeTypeCaption, Long participantId, */
			B3Outcome outcome,
			ArrayList<RevisionedEntity<B3EventInfo>> statusList,
			ArrayList<RevisionedEntity<B3EventInfo>> scoreList, 
			ArrayList<RevisionedEntity<B3BettingOffer>> offerList,
			PrintStream out) {
		
		/*String caption = "Detailed Odds Table: " + outcomeTypeCaption;
		if (participantId != null) {
				caption += " (participant id: " + participantId + ")";
		}*/
		
		String caption;
		if (outcome == null) {
			caption = "Statuses & scores";
		} else {
			String s = append(null, "paramBoolean1", outcome.entity.getParamBoolean1());
			s = append(s, "paramEventPartId1", outcome.entity.getParamEventPartId1());
			s = append(s, "paramFloat1", outcome.entity.getParamFloat1());
			s = append(s, "paramFloat2", outcome.entity.getParamFloat2());
			s = append(s, "paramFloat3", outcome.entity.getParamFloat3());
			s = append(s, "paramParticipantId1", outcome.entity.getParamParticipantId1());
			s = append(s, "paramParticipantId2", outcome.entity.getParamParticipantId2());
			s = append(s, "paramParticipantId3", outcome.entity.getParamParticipantId3());
			s = append(s, "paramString1", outcome.entity.getParamString1());
			//long outcomeId = offerList.get(0).b3entity.entity.getOutcomeId();
			caption = "Detailed Odds Table: " + outcome.type.entity.getName() +
					" (" + s + ")";
		}
		if (out == null) {
			data = new DetailedOddsTableData();
			data.setCaption(caption);
		}
		
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
		if (offerList != null) {
			for (RevisionedEntity<B3BettingOffer> one : offerList) {
				odds.add(new DetailedOddsItemOffer(one));
				timeset.add(one.time);
			}
		}
		
		timePoints.addAll(timeset);
		Collections.sort(timePoints);
		
		odds.prepare();
		scores.prepare();
		statuses.prepare();
		
		if (out != null) {
			out.println(caption + "<br/>");
			out.print("Time | ");
		}
		for (Provider p : odds.providers) {
			if (out == null) {
				data.oddsProviderNames.put(p.getId(), p.getName());
			} else {
				out.print("Odds from " + p.getName() + " | ");
			}
		}
		for (Provider p : scores.providers) {
			if (out == null) {
				data.scoreProviderNames.put(p.getId(), p.getName());
			} else {
				out.print("Score from " + p.getName() + " | ");
			}
		}
		for (Provider p : statuses.providers) {
			if (out == null) {
				data.statusProviderNames.put(p.getId(), p.getName());
			} else {
				out.print("Match status from " + p.getName() + " | ");
			}
		}
		
		if (out != null) {
			out.println("</br>");
		}
		
		for (Long time : timePoints) {
			if (time == 0) {
				continue;
			}
			DetailedOddsTableRow row = new DetailedOddsTableRow();
			row.setTime(time);
			if (out == null) {
				data.addRow(row);
			} else {
				out.print(time + ": " + new Date(time) + " | ");
			}
			for (int i = 0; i < odds.providers.length; i++) {
				if (out == null) {
					row.addOdds(odds.providers[i].getId(), odds.getValue(time, i));
				} else {
					out.print(odds.getValue(time, i) + " | ");
				}
			}
			for (int i = 0; i < scores.providers.length; i++) {
				if (out == null) {
					row.addScore(scores.providers[i].getId(), scores.getValue(time, i));
				} else {
					out.print(scores.getValue(time, i++) + " | ");
				}
			}
			for (int i = 0; i < statuses.providers.length; i++) {
				if (out == null) {
					row.addStatus(statuses.providers[i].getId(), statuses.getValue(time, i));
				} else {
					out.print(statuses.getValue(time, i++) + " | ");
				}
			}
			
			if (out != null) {
				out.println("<br/>");
			}
		}
		if (out != null) {
			out.println("<br/>");
		}
	}

}

class DetailedOddsColumnSet {
	
	String typeName;
	private HashMap<Provider, TreeMap<Long, DetailedOddsItem<?>>> providerMap = new HashMap<>();
	
	Provider[] providers;
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
		providers = new Provider[providerMap.size()];
		itemsByProvider = new TreeMap[providerMap.size()];
		int index = 0;
		for (Entry<Provider, TreeMap<Long, DetailedOddsItem<?>>> entry : providerMap.entrySet()) {
			providers[index] = entry.getKey();
			itemsByProvider[index] = entry.getValue();
			index++;
		}
	}
	
	String getValue(long time, int index) {
		DetailedOddsItem<?> item = itemsByProvider[index].get(time);
		if (item == null) {
			if (lastItem == null) {
				return "";
			} else {
				return lastItem.getValue();
				//return "(" + lastItem.getValue() + ")";
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
	
	abstract Provider getProvider();
	
	abstract String getValue();
}

class DetailedOddsItemStatus extends DetailedOddsItem<B3EventInfo> {
	
	private static String[] CAPTIONS = {"Pending", "In Progress", "Ended"};

	public DetailedOddsItemStatus(RevisionedEntity<B3EventInfo> revisionedEntity) {
		super(revisionedEntity);
	}

	@Override
	Provider getProvider() {
		return revisionedEntity.b3entity.provider.entity;
	}

	/*@Override
	String getProviderName() {
		if (revisionedEntity.b3entity.provider == null) {
			return revisionedEntity.b3entity.entity.getProviderId() + "";
		}
		return revisionedEntity.b3entity.provider.entity.getName();
	}*/

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
	Provider getProvider() {
		return revisionedEntity.b3entity.provider.entity;
	}

	/*@Override
	String getProviderName() {
		if (revisionedEntity.b3entity.provider == null) {
			return revisionedEntity.b3entity.entity.getProviderId() + "";
		}
		return revisionedEntity.b3entity.provider.entity.getName();
	}*/

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
	Provider getProvider() {
		return revisionedEntity.b3entity.provider.entity;
	}

	/*@Override
	String getProvider() {
		if (this.revisionedEntity.b3entity.provider == null) {
			return revisionedEntity.b3entity.entity.getProviderId() + "";
		}
		return this.revisionedEntity.b3entity.provider.entity.getName();
	}*/
	
	/*private static String[] statuses = new String[] {
			"Standard", "Starting Price", "Non-Participant", "Removed", "Invalid", "Resolved"
	};*/

	@Override
	String getValue() {
		if (revisionedEntity.b3entity.status.entity.getIsAvailable()) {
			return revisionedEntity.b3entity.entity.getOdds() + "";
		} else {
			//return statuses[(int) revisionedEntity.b3entity.entity.getStatusId()];
			return revisionedEntity.b3entity.status.entity.getName();
		}
		/*int status = (int) revisionedEntity.b3entity.entity.getStatusId();
		if (status == 1 || status == 2) {
			return revisionedEntity.b3entity.entity.getOdds() + "";
		} else {
			return statuses[status - 1];
		}*/
	}
	
}