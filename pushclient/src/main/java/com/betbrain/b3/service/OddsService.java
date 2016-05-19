package com.betbrain.b3.service;

import java.util.Date;

import javax.ws.rs.DefaultValue;
import javax.ws.rs.GET;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;
import javax.ws.rs.Produces;
import javax.ws.rs.QueryParam;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;

import com.betbrain.b3.api.B3Engine;
import com.betbrain.b3.pushclient.JsonMapper;
import com.betbrain.b3.report.IDs;
import com.betbrain.b3.report.detailedodds.DetailedOddsTable2;
import com.betbrain.sepc.connector.sportsmodel.Event;
 
@Path("/b3")
public class OddsService {
	
	private static B3Engine b3 = new B3Engine();
	
	public static void main(String[] args) {
		//DynamoWorker.initBundleCurrent();
		new OddsService().detailedOddsTable(217633296L, 3L, IDs.BETTINGTYPE_1X2, 
				null, null, null, null, null, true, "text");
	}
	
	@GET
	@Path("/sports")
	@Produces({MediaType.TEXT_PLAIN})
	public Response listSports() {
		JsonMapper mapper = new JsonMapper();
		return Response.status(200).entity(mapper.serialize(b3.searchSports())).build();
	}
	
	@GET
	@Path("/countries")
	@Produces({MediaType.TEXT_PLAIN})
	public Response listCountries() {
		JsonMapper mapper = new JsonMapper();
		return Response.status(200).entity(mapper.serialize(b3.listCountries())).build();
	}
	
	@GET
	@Path("/countries/{countryId}/leagues")
	@Produces({MediaType.TEXT_PLAIN})
	public Response listLeagues(@PathParam("countryId") Long countryId, @QueryParam("sportId") Long sportId) {
		JsonMapper mapper = new JsonMapper();
		return Response.status(200).entity(mapper.serialize(b3.searchLeagues(sportId, countryId))).build();
	}
	
	@GET
	@Path("/leagues/{leagueId}/matches")
	@Produces({MediaType.TEXT_PLAIN})
	public Response listMatches(@PathParam("leagueId") long leagueId, 
			@QueryParam("fromDate") Date fromTime, @QueryParam("toDate") Date toTime) {
		/*B3KeyEvent eventKey = new B3KeyEvent(leagueId, IDs.EVENTTYPE_GENERICMATCH, (String) null);
		@SuppressWarnings("unchecked")
		ArrayList<B3Event> eventIds = (ArrayList<B3Event>) eventKey.listEntities(false, mapper);*/
		Event[] matches = b3.searchMatches(leagueId, fromTime, toTime);
		/*String s = "";
		for (Event e : matches) {
			s += e.getId() + "\n";
		}*/
		JsonMapper mapper = new JsonMapper();
		return Response.status(200).entity(mapper.serialize(matches)).build();
	}
	
	@GET
	@Path("/detailedoddstable/{matchid}")
	@Produces({MediaType.APPLICATION_JSON, MediaType.TEXT_PLAIN})
	public Response detailedOddsTable(@PathParam("matchid") long matchId, 
			@QueryParam("eventPartId") Long eventPartId,
			@QueryParam("bettingTypeId") Long bettingTypeId,
			@QueryParam("paramFloat1") Float paramFloat1,
			@QueryParam("paramFloat2") Float paramFloat2,
			@QueryParam("paramFloat3") Float paramFloat3,
			@QueryParam("paramBoolean1") Boolean paramBoolean1,
			@QueryParam("paramString1") String paramString1,
			@DefaultValue("false") @QueryParam("prettyPrint") boolean prettyPrint,
			//@HeaderParam("Accept") String accepted) {
			@QueryParam("format") String format) {

		if (eventPartId == null) {
			return Response.status(400).entity("Missing eventPartId").build();
		}
		if (bettingTypeId == null) {
			return Response.status(400).entity("Missing bettingTypeId").build();
		}
		boolean plainText = false;
		/*if (accepted != null && !"".equals(accepted.trim())) {
			plainText = MediaType.valueOf(accepted) == MediaType.TEXT_PLAIN_TYPE;
		}*/
		if (format != null && "text".equalsIgnoreCase(format)) {
			plainText = true;
		}
		
		DetailedOddsTable2 report = new DetailedOddsTable2(b3, matchId, eventPartId, bettingTypeId,
				paramFloat1, paramFloat2, paramFloat3, paramBoolean1, paramString1);
		report.setPlainText(plainText);
		report.run();
		
		if (!plainText) {
			//LinkedList<DetailedOddsTableData> reportData = report.outputData;
			//return reportData.toArray(new DetailedOddsTableData[reportData.size()]);
			JsonMapper mapper = new JsonMapper();
			mapper.prettyPrint(prettyPrint);
			return Response.status(200).entity(mapper.deepSerialize(report.outputData)).build();
		} else {
			return Response.status(200).entity(new String(report.outStream.toByteArray())).build();
		}
	}
 
}