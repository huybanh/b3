package com.betbrain.b3.api;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.Date;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedList;

import com.betbrain.b3.data.B3Key;
import com.betbrain.b3.data.B3KeyEntity;
import com.betbrain.b3.data.B3KeyEvent;
import com.betbrain.b3.data.B3KeyLink;
import com.betbrain.b3.data.B3KeyOffer;
import com.betbrain.b3.data.B3Table;
import com.betbrain.b3.data.DynamoWorker;
import com.betbrain.b3.data.EntityLinkSourcePart;
import com.betbrain.b3.model.B3BettingOffer;
import com.betbrain.b3.model.B3Event;
import com.betbrain.b3.model.B3Location;
import com.betbrain.b3.model.B3OutcomeTypeBettingTypeRelation;
import com.betbrain.b3.model.B3Sport;
import com.betbrain.b3.pushclient.JsonMapper;
import com.betbrain.b3.report.IDs;
import com.betbrain.b3.report.detailedodds.DetailedOddsTable2;
import com.betbrain.sepc.connector.sportsmodel.BettingOffer;
import com.betbrain.sepc.connector.sportsmodel.BettingType;
import com.betbrain.sepc.connector.sportsmodel.Event;
import com.betbrain.sepc.connector.sportsmodel.EventPart;
import com.betbrain.sepc.connector.sportsmodel.Location;
import com.betbrain.sepc.connector.sportsmodel.Outcome;
import com.betbrain.sepc.connector.sportsmodel.OutcomeTypeBettingTypeRelation;
import com.betbrain.sepc.connector.sportsmodel.Sport;

/**
 * B3Engine provides high level API to access data, reports in AWS dynamodb.
 * 
 * Instantiation of B3Engine is heavy. Clients are recommended to instantiate B3Engine once,
 * and reuse the instance for all subsequent requests.
 * 
 * Thread safety: all methods of B3Engine are thread-safe
 * 
 * @author huybanh
 */
public class B3Engine implements B3Api {
	
	private final HashMap<Long, LinkedList<Long>> outcomeTypesByBettingType = new HashMap<>();
	//private final HashMap<Long, LinkedList<Long>> bettingTypesByOutcomeType = new HashMap<>();
	
	public static void main(String[] args) {
		B3Api b3 = new B3Engine();
		//b3.searchSports();
		b3.searchCountries(null);
		//b3.searchLeagues(null, null);
		//System.out.println("Matches");
		//b3.searchMatches(215754838, null, null);
		//((B3Engine) b3).searchBettingTypes2(219900664L);
		//b3.searchBettingTypes(219900664L);
		//b3.searchEventParts(219900664L, null);
		//OutcomeParameter[][] result = b3.searchParameters(219464997L, 177L, IDs.EVENTPART_ORDINARYTIME);
		//LinkedList<DetailedOddsTableTrait> result = b3.reportDetailedOddsTable(219464997L, 3L, 177L, null);
		
		/*int i = 0;
		for (Object o : result) {
			System.out.println(i++ + ": " + o);
		}*/
	}

	public B3Engine() {
		DynamoWorker.initBundleCurrent();
		
		B3KeyEntity entityKey = new B3KeyEntity(OutcomeTypeBettingTypeRelation.class);
		JsonMapper jsonMapper = new JsonMapper();
		@SuppressWarnings("unchecked")
		ArrayList<B3OutcomeTypeBettingTypeRelation> relations = 
				(ArrayList<B3OutcomeTypeBettingTypeRelation>) entityKey.listEntities(false, B3OutcomeTypeBettingTypeRelation.class, jsonMapper);
		for (B3OutcomeTypeBettingTypeRelation one : relations) {
			//System.out.println(one.entity.getId() + ": " + one.entity.getBettingTypeId() + "->" + one.entity.getOutcomeTypeId());
			LinkedList<Long> outcomeTypeIdList = outcomeTypesByBettingType.get(one.entity.getBettingTypeId());
			if (outcomeTypeIdList == null) {
				outcomeTypeIdList = new LinkedList<>();
				outcomeTypesByBettingType.put(one.entity.getBettingTypeId(), outcomeTypeIdList);
			}
			outcomeTypeIdList.add(one.entity.getOutcomeTypeId());
			
			/*LinkedList<Long> bettingTypeIdList = bettingTypesByOutcomeType.get(one.entity.getOutcomeTypeId());
			if (bettingTypeIdList == null) {
				bettingTypeIdList = new LinkedList<>();
				bettingTypesByOutcomeType.put(one.entity.getOutcomeTypeId(), bettingTypeIdList);
			}
			bettingTypeIdList.add(one.entity.getBettingTypeId());*/
		}
		
		/*for (Entry<Long, LinkedList<Long>> e : outcomeTypesByBettingType.entrySet()) {
			System.out.println("Betting " + e.getKey() + "->" + e.getValue());
		}
		for (Entry<Long, LinkedList<Long>> e : bettingTypesByOutcomeType.entrySet()) {
			System.out.println("Outcome " + e.getKey() + "->" + e.getValue());
		}*/
	}
	
	public Long[] getOutcomeTypeIds(long bettingTypeId) {
		LinkedList<Long> idList = outcomeTypesByBettingType.get(bettingTypeId);
		if (idList == null) {
			return new Long[0];
		} else {
			return idList.toArray(new Long[idList.size()]);
		}
	}
	
	/*public Long[] getBettingTypeIds(long outcomeTypeId) {
		LinkedList<Long> idList = bettingTypesByOutcomeType.get(outcomeTypeId);
		if (idList == null) {
			return new Long[0];
		} else {
			return idList.toArray(new Long[idList.size()]);
		}
	}*/
	
	/* (non-Javadoc)
	 * @see com.betbrain.b3.api.B3Api#listSports()
	 */
	@Override
	public Sport[] searchSports() {
		JsonMapper jsonMapper = new JsonMapper();
		B3KeyEntity entityKey = new B3KeyEntity(Sport.class);
		@SuppressWarnings("unchecked")
		ArrayList<B3Sport> allSports = (ArrayList<B3Sport>) entityKey.listEntities(false, B3Sport.class, jsonMapper);
		Collections.sort(allSports, new Comparator<B3Sport>() {

			@Override
			public int compare(B3Sport o1, B3Sport o2) {
				return o1.entity.getName().compareTo(o2.entity.getName());
			}
		});
		Sport[] result = new Sport[allSports.size()];
		int index = 0;
		for (Object one : allSports) {
			result[index] = ((B3Sport) one).entity;
			System.out.println(result[index]);
			index++;
		}
		return result;
	}
	
	public Location[] listCountries() {
		JsonMapper jsonMapper = new JsonMapper();
		B3KeyEntity entityKey = new B3KeyEntity(Location.class);
		ArrayList<?> allLocs = entityKey.listEntities(false, B3Location.class, jsonMapper);
		HashSet<Location> result = new HashSet<>();
		Iterator<?> it = allLocs.iterator();
		while (it.hasNext()) {
			Location loc = ((B3Location) it.next()).entity;
			if (loc.getTypeId() == IDs.LOCATIONTYPE_COUNTRY) {
				result.add(loc);
			}
		}
		return result.toArray(new Location[result.size()]);
	}

	/* (non-Javadoc)
	 * @see com.betbrain.b3.api.B3Api#listCountries()
	 */
	@Override
	public Location[] searchCountries(Long sportId) {
		JsonMapper jsonMapper = new JsonMapper();
		B3KeyEvent eventKey = new B3KeyEvent(null, IDs.EVENTTYPE_GENERICTOURNAMENT, (String) null);
		ArrayList<?> allLeagues = eventKey.listEntities(false, jsonMapper, 
				B3Table.CELL_LOCATOR_THIZ, Event.PROPERTY_NAME_venueId);
		HashSet<Location> countrySet = new HashSet<>();
		Iterator<?> it = allLeagues.iterator();
		while (it.hasNext()) {
			B3Event e = (B3Event) it.next();
			if ((sportId == null || e.entity.getSportId() == sportId) &&
					e.venue.entity.getTypeId() == IDs.LOCATIONTYPE_COUNTRY) {
				countrySet.add(e.venue.entity);
			}
		}
		System.out.println("Leagues/countries: " + allLeagues.size() + "/" + countrySet.size());
		LinkedList<Location> countries = new LinkedList<>(countrySet);
		Collections.sort(countries, new Comparator<Location>() {

			@Override
			public int compare(Location o1, Location o2) {
				return o1.getName().compareTo(o2.getName());
			}
		});
		return countries.toArray(new Location[countries.size()]);
	}
	
	/*public Location[] getCountries() {
		JsonMapper jsonMapper = new JsonMapper();
		B3KeyLink linkKey = new B3KeyLink(B3LocationType.class, IDs.LOCATIONTYPE_COUNTRY, 
				B3Location.class, Location.PROPERTY_NAME_typeId);
		ArrayList<?> allLocs = linkKey.listLinks();
		Location[] result = new Location[allLocs.size()];
		int index = 0;
		for (Object one : allLocs) {
			Location loc = ((B3Location) one).entity;
			if (loc.getTypeId() == IDs.LOCATIONTYPE_COUNTRY) {
				result[index] = loc;
				System.out.println(result[index]);
			}
			index++;
		}
		return result;
	}*/
	
	/* (non-Javadoc)
	 * @see com.betbrain.b3.api.B3Api#searchLeagues(java.lang.Long, java.lang.Long)
	 */
	@Override
	public Event[] searchLeagues(Long sportId, Long countryId) {
		JsonMapper jsonMapper = new JsonMapper();
		B3KeyEvent eventKey = new B3KeyEvent(null, IDs.EVENTTYPE_GENERICTOURNAMENT, (String) null);
		ArrayList<?> allLeagues = eventKey.listEntities(false, jsonMapper, B3Table.CELL_LOCATOR_THIZ);
		LinkedList<Event> result = new LinkedList<>();
		Iterator<?> it = allLeagues.iterator();
		while (it.hasNext()) {
			Event e = ((B3Event) it.next()).entity;
			if ((sportId == null || e.getSportId() == sportId)
					&& (countryId == null || e.getVenueId() == countryId)) {
				result.add(e);
			}
		}
		return result.toArray(new Event[result.size()]);
	}
	
	/* (non-Javadoc)
	 * @see com.betbrain.b3.api.B3Api#searchMatches(long, java.util.Date, java.util.Date)
	 */
	@Override
	public Event[] searchMatches(long leagueId, Date fromTime, Date toTime) {
		JsonMapper jsonMapper = new JsonMapper();
		String fromTimeString = null;
		if (fromTime != null) {
			fromTimeString = B3Key.dateFormat.format(fromTime);
		}
		String toTimeString = null;
		if (toTime != null) {
			toTimeString = B3Key.dateFormat.format(toTime);
		}
		B3KeyEvent eventKey = new B3KeyEvent(leagueId, IDs.EVENTTYPE_GENERICMATCH, fromTimeString);
		eventKey.rangeKeyEnd = toTimeString;
		
		@SuppressWarnings("unchecked")
		ArrayList<B3Event> matches = (ArrayList<B3Event>) eventKey.listEntities(false, jsonMapper, B3Table.CELL_LOCATOR_THIZ);
		Event[] result = new Event[matches.size()];
		int index = 0;
		for (Object one : matches) {
			result[index] = ((B3Event) one).entity;
			System.out.println(result[index]);
			index++;
		}
		return result;
	}
	
	/* (non-Javadoc)
	 * @see com.betbrain.b3.api.B3Api#listBettingTypes()
	 */
	/*@Override
	public BettingType[] searchBettingTypes(long matchId) {
		JsonMapper jsonMapper = new JsonMapper();
		B3KeyOffer offerKey = new B3KeyOffer(matchId, null, null, null, null, null);
		@SuppressWarnings("unchecked")
		ArrayList<B3BettingOffer> offers = (ArrayList<B3BettingOffer>) offerKey.listEntities(
				false, jsonMapper, B3Table.CELL_LOCATOR_THIZ, BettingOffer.PROPERTY_NAME_bettingTypeId);
		
		HashSet<BettingType> result = new HashSet<>();
		for (B3BettingOffer one : offers) {
			//System.out.println("Offer range key: " + one);
			result.add(one.bettingType.entity);
		}
		System.out.println(result.size());
		return result.toArray(new BettingType[result.size()]);
	}*/
	
	/* (non-Javadoc)
	 * @see com.betbrain.b3.api.B3Api#searchBettingTypes(long)
	 */
	@Override
	public BettingType[] searchBettingTypes(long matchId) {
		JsonMapper jsonMapper = new JsonMapper();
		B3KeyLink linkKey = new B3KeyLink(BettingType.class, null, new EntityLinkSourcePart(Event.class, matchId));
		ArrayList<Long> bettingTypeIds = linkKey.listLinks();
		ArrayList<BettingType> result = B3KeyEntity.load(jsonMapper, BettingType.class, bettingTypeIds);
		return result.toArray(new BettingType[result.size()]);
	}
	
	/*public BettingType[] searchBettingTypes2(Long eventId) {
		JsonMapper jsonMapper = new JsonMapper();
		B3KeyOutcome outcomeKey = new B3KeyOutcome(eventId, null, null, null);
		@SuppressWarnings("unchecked")
		ArrayList<B3Outcome> outcomes = (ArrayList<B3Outcome>) outcomeKey.listEntities(false, jsonMapper);
		HashSet<Long> result = new HashSet<>();
		for (B3Outcome one : outcomes) {
			long outcomeTypeId = one.entity.getTypeId();
			LinkedList<Long> bettingTypeIdList = bettingTypesByOutcomeType.get(outcomeTypeId);
			result.addAll(bettingTypeIdList);
		}
		System.out.println(result.size());
		return null;
	}*/
	
	/*public BettingType[] listBettingTypes() {
		JsonMapper jsonMapper = new JsonMapper();
		B3KeyEntity entityKey = new B3KeyEntity(BettingType.class);
		ArrayList<?> allSports = entityKey.listEntities(false, B3BettingType.class, jsonMapper);
		BettingType[] result = new BettingType[allSports.size()];
		int index = 0;
		for (Object one : allSports) {
			result[index] = ((B3BettingType) one).entity;
			System.out.println(result[index]);
			index++;
		}
		return result;
	}*/
	
	/* (non-Javadoc)
	 * @see com.betbrain.b3.api.B3Api#searchEventParts(long, java.lang.Long)
	 */
	@Override
	public EventPart[] searchEventParts(long matchId, long bettingTypeId) {
		/*JsonMapper jsonMapper = new JsonMapper();
		B3KeyOffer offerKey = new B3KeyOffer(matchId, bettingTypeId, null, null, null, null);
		@SuppressWarnings("unchecked")
		ArrayList<B3BettingOffer> offers = (ArrayList<B3BettingOffer>) offerKey.listEntities(
				false, jsonMapper, B3Table.CELL_LOCATOR_THIZ, BettingOffer.PROPERTY_NAME_outcomeId,
				BettingOffer.PROPERTY_NAME_outcomeId + B3Table.CELL_LOCATOR_SEP + Outcome.PROPERTY_NAME_eventPartId);
		HashSet<EventPart> result = new HashSet<>();
		for (B3BettingOffer one : offers) {
			result.add(one.outcome.eventPart.entity);
		}
		System.out.println(result.size());
		return result.toArray(new EventPart[result.size()]);*/
		
		JsonMapper jsonMapper = new JsonMapper();
		B3KeyLink linkKey = new B3KeyLink(EventPart.class, null, 
				new EntityLinkSourcePart(Event.class, matchId), new EntityLinkSourcePart(BettingType.class, bettingTypeId));
		ArrayList<Long> eventPartIds = linkKey.listLinks();
		ArrayList<EventPart> result = B3KeyEntity.load(jsonMapper, EventPart.class, eventPartIds);
		return result.toArray(new EventPart[result.size()]);
	}
	
	/*public EventPart[] searchEventParts2(long matchId, Long bettingTypeId) {
		JsonMapper jsonMapper = new JsonMapper();
		B3KeyOutcome outcomeKey = new B3KeyOutcome(matchId, null, null, null);
		@SuppressWarnings("unchecked")
		ArrayList<B3Outcome> outcomes = (ArrayList<B3Outcome>) outcomeKey.listEntities(
				false, jsonMapper, B3Table.CELL_LOCATOR_THIZ, Outcome.PROPERTY_NAME_eventPartId);
		HashSet<EventPart> result = new HashSet<>();
		LinkedList<Long> preferedOutcomeTypes = outcomeTypesByBettingType.get(bettingTypeId);
		for (B3Outcome one : outcomes) {
			if (one.entity.getIsNegation()) {
				continue;
			}
			if (!preferedOutcomeTypes.contains(one.entity.getTypeId())) {
				continue;
			}
			result.add(one.eventPart.entity);
		}
		System.out.println(result.size());
		return result.toArray(new EventPart[result.size()]);
	}*/
	
	@SuppressWarnings("unchecked")
	@Override
	public OutcomeParameter[][] searchParameters(long matchId, long bettingTypeId, long eventPartId) {
		
		JsonMapper jsonMapper = new JsonMapper();
		//B3KeyOffer offerKey = new B3KeyOffer(eventId, eventPartId, outcomeTypeId, outcomeId, bettingTypeId, offerId);
		B3KeyOffer offerKey = new B3KeyOffer(matchId, bettingTypeId, eventPartId, null, null, null);
		ArrayList<B3BettingOffer> offers = (ArrayList<B3BettingOffer>) offerKey.listEntities(
				false, jsonMapper, B3Table.CELL_LOCATOR_THIZ, BettingOffer.PROPERTY_NAME_outcomeId);
		
		ArrayList<Outcome> allOutcomes = new ArrayList<>();
		for (B3BettingOffer one : offers) {
			allOutcomes.add(one.outcome.entity);
		}
		
		HashSet<HashSet<OutcomeParameter>> setOfSets = new HashSet<>();
		for (Outcome one : allOutcomes) {
			if (one.getIsNegation()) {
				continue;
			}
			System.out.println("Looking for param in outcome: " + one);
			HashSet<OutcomeParameter> paramSet = extractParameters(one);
			if (!paramSet.isEmpty()) {
				setOfSets.add(paramSet);
			}
		}
		
		OutcomeParameter[][] result = new OutcomeParameter[setOfSets.size()][];
		int i = 0;
		for (HashSet<OutcomeParameter> oneSet : setOfSets) {
			result[i] = oneSet.toArray(new OutcomeParameter[oneSet.size()]);
			i++;
		}
		//System.out.println(result.size());
		return result;
	}

	/*@SuppressWarnings("unchecked")
	public HashSet<OutcomeParameter>[] searchParameters2(Long matchId, Long bettingTypeId, Long eventPartId) {
		
		JsonMapper jsonMapper = new JsonMapper();
		LinkedList<Long> preferedOutcomeTypes = outcomeTypesByBettingType.get(bettingTypeId);
		ArrayList<B3Outcome> allOutcomes = new ArrayList<>();
		
		for (Long outcomeTypeId : preferedOutcomeTypes) {
			B3KeyOutcome outcomeKey = new B3KeyOutcome(matchId, eventPartId, outcomeTypeId, null);
			ArrayList<B3Outcome> outcomesByType = (ArrayList<B3Outcome>) outcomeKey.listEntities(
					false, jsonMapper, B3Table.CELL_LOCATOR_THIZ);
			allOutcomes.addAll(outcomesByType);
		}
		
		HashSet<HashSet<OutcomeParameter>> result = new HashSet<>();
		for (B3Outcome one : allOutcomes) {
			if (one.entity.getIsNegation()) {
				continue;
			}
			System.out.println(one.entity);
			HashSet<OutcomeParameter> paramSet = extractParameters(one.entity);
			if (!paramSet.isEmpty()) {
				result.add(paramSet);
			}
		}
		//System.out.println(result.size());
		return result.toArray(new HashSet[result.size()]);
	}*/
	
	private static HashSet<OutcomeParameter> extractParameters(Outcome outcome) {
		HashSet<OutcomeParameter> paramSet = new HashSet<>();
		addParam(paramSet, "paramBoolean1", outcome.getParamBoolean1());
		addParam(paramSet, "paramString1", outcome.getParamString1());
		addParam(paramSet, "paramFloat1", outcome.getParamFloat1());
		addParam(paramSet, "paramFloat2", outcome.getParamFloat2());
		addParam(paramSet, "paramFloat3", outcome.getParamFloat3());
		System.out.println("Extracted params: " + paramSet);
		return paramSet;
	}
	
	private static void addParam(HashSet<OutcomeParameter> paramSet, String name, Object value) {
		if (value == null) {
			return;
		}
		paramSet.add(new OutcomeParameter(name, String.valueOf(value)));
	}
	
	/* (non-Javadoc)
	 * @see com.betbrain.b3.api.B3Api#reportDetailedOddsTable(long, long, long, java.lang.Float, java.lang.Float, java.lang.Float, java.lang.Boolean, java.lang.String)
	 */
	@Override
	public LinkedList<DetailedOddsTableTrait> reportDetailedOddsTable(long matchId, long eventPartId, long bettingTypeId,
			OutcomeParameter[] outcomeParams) {
		DetailedOddsTable2 report = new DetailedOddsTable2(this, matchId, eventPartId, bettingTypeId, outcomeParams);
		report.run();
		return report.outputData;
	}
}
