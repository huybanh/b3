package com.betbrain.b3.service;

import java.util.ArrayList;

import javax.ws.rs.DefaultValue;
import javax.ws.rs.GET;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;
import javax.ws.rs.Produces;
import javax.ws.rs.QueryParam;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;

import com.betbrain.b3.data.B3KeyEvent;
import com.betbrain.b3.data.DynamoWorker;
import com.betbrain.b3.model.B3Event;
import com.betbrain.b3.pushclient.JsonMapper;
import com.betbrain.b3.report.IDs;
import com.betbrain.b3.report.detailedodds.DetailedOddsTable2;
 
@Path("/odds")
public class OddsService {
	
	private JsonMapper mapper = new JsonMapper();
	
	public static void main(String[] args) {
		DynamoWorker.initBundleCurrent();
		new OddsService().detailedOddsTable(217633296, 3, true, "text");
	}
	
	@GET
	@Path("league/{leagueId}/matchIds")
	@Produces({MediaType.APPLICATION_JSON, MediaType.TEXT_PLAIN})
	public Response listMatches(@PathParam("leagueId") long leagueId) {
		B3KeyEvent eventKey = new B3KeyEvent(leagueId, IDs.EVENTTYPE_GENERICMATCH, (String) null);
		@SuppressWarnings("unchecked")
		ArrayList<B3Event> eventIds = (ArrayList<B3Event>) eventKey.listEntities(false, mapper);
		
		String s = "";
		for (B3Event e : eventIds) {
			s += e.entity.getId() + "\n";
		}
		return Response.status(200).entity(s).build();
	}
	
	@GET
	@Path("/detailedtable/{matchid}")
	@Produces({MediaType.APPLICATION_JSON, MediaType.TEXT_PLAIN})
	public Response detailedOddsTable(@PathParam("matchid") long matchId, 
			@DefaultValue("3") @QueryParam("eventPartId") long eventPartId,
			@DefaultValue("false") @QueryParam("prettyPrint") boolean prettyPrint,
			//@HeaderParam("Accept") String accepted) {
			@QueryParam("format") String format) {

		boolean plainText = false;
		/*if (accepted != null && !"".equals(accepted.trim())) {
			plainText = MediaType.valueOf(accepted) == MediaType.TEXT_PLAIN_TYPE;
		}*/
		if (format != null && "text".equalsIgnoreCase(format)) {
			plainText = true;
		}
		DetailedOddsTable2 report = new DetailedOddsTable2(matchId, eventPartId, plainText, mapper);
		report.run();
		
		if (!plainText) {
			//LinkedList<DetailedOddsTableData> reportData = report.outputData;
			//return reportData.toArray(new DetailedOddsTableData[reportData.size()]);
			mapper.prettyPrint(prettyPrint);
			return Response.status(200).entity(mapper.deepSerialize(report.outputData)).build();
		} else {
			return Response.status(200).entity(new String(report.outStream.toByteArray())).build();
		}
	}
 
}