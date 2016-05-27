package com.betbrain.b3.api;

import java.util.Date;
import java.util.LinkedList;

import com.betbrain.sepc.connector.sportsmodel.BettingType;
import com.betbrain.sepc.connector.sportsmodel.Event;
import com.betbrain.sepc.connector.sportsmodel.EventPart;
import com.betbrain.sepc.connector.sportsmodel.Location;
import com.betbrain.sepc.connector.sportsmodel.Sport;

/**
 * 
 * B3Engine provides high level API to access data, reports in AWS dynamodb.
 * 
 * Instantiation of B3Engine is heavy. Clients are recommended to instantiate B3Engine once,
 * and reuse the instance for all subsequent calls.
 * 
 * Thread safety: all methods of B3Engine are thread-safe
 * 
 * @author huybanh
 *
 */
public interface B3Api {

	/**
	 * @return all sports
	 */
	Sport[] searchSports();

	/**
	 * @param sportId
	 * @return countries have given sportId, or all countries if null sportId
	 */
	Location[] searchCountries(Long sportId);

	/**
	 * @param sportId
	 * @param countryId
	 * @return tournaments for given sportId at given countryId
	 */
	Event[] searchLeagues(Long sportId, Long countryId);

	/**
	 * @param leagueId
	 * @param fromTime inclusive or null if no fromTime constraint
	 * @param toTime exclusive or null if no toTime constraint
	 * @return matches belong to given leagueId, with start time in between fromTime inclusive and toTime exclusive.
	 * 
	 */
	Match[] searchMatches(long leagueId, Date fromTime, Date toTime);

	/**
	 * @param matchId
	 * @return betting type IDs which are available for the given matchId
	 */
	BettingType[] searchBettingTypes(long matchId);
	
	/**
	 * @param matchId
	 * @param bettingTypeId
	 * @return event part IDs for the given combination of matchId and bettingTypeId
	 */
	EventPart[] searchEventParts(long matchId, long bettingTypeId);
	
	/**
	 * @param matchId
	 * @param bettingTypeId
	 * @param eventPartId
	 * @return available parameters for given matchId, bettingTypeId and eventPartId
	 */
	OutcomeParameter[][] searchParameters(long matchId, long bettingTypeId, long eventPartId);

	/**
	 * @param matchId
	 * @param eventPartId
	 * @param bettingTypeId
	 * @param outcomeParams
	 * @return Detailed odds report
	 */
	LinkedList<DetailedOddsTableTrait> reportDetailedOddsTable(long matchId, long eventPartId, long bettingTypeId,
			OutcomeParameter[] outcomeParams);

}