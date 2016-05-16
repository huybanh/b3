package com.betbrain.b3.report.detailedodds;

import java.util.ArrayList;
import java.util.HashMap;

import javax.xml.bind.annotation.XmlRootElement;

import com.betbrain.sepc.connector.util.beans.BeanUtil;

@XmlRootElement
public class DetailedOddsTableData {
	
	private String caption;
	
	HashMap<Long, String> oddsProviderNames = new HashMap<>();
	HashMap<Long, String> scoreProviderNames = new HashMap<>();
	HashMap<Long, String> statusProviderNames = new HashMap<>();

	private ArrayList<DetailedOddsTableRow> rows = new ArrayList<>();
	
	void addRow(DetailedOddsTableRow row) {
		rows.add(row);
	}

	public String getCaption() {
		return caption;
	}

	public void setCaption(String caption) {
		this.caption = caption;
	}

	public HashMap<Long, String> getOddsProviderNames() {
		return oddsProviderNames;
	}

	public void setOddsProviderNames(HashMap<Long, String> oddsProviderNames) {
		this.oddsProviderNames = oddsProviderNames;
	}

	public HashMap<Long, String> getScoreProviderNames() {
		return scoreProviderNames;
	}

	public void setScoreProviderNames(HashMap<Long, String> scoreProviderNames) {
		this.scoreProviderNames = scoreProviderNames;
	}

	public HashMap<Long, String> getStatusProviderNames() {
		return statusProviderNames;
	}

	public void setStatusProviderNames(HashMap<Long, String> statusProviderNames) {
		this.statusProviderNames = statusProviderNames;
	}

	public ArrayList<DetailedOddsTableRow> getRows() {
		return rows;
	}

	public void setRows(ArrayList<DetailedOddsTableRow> rows) {
		this.rows = rows;
	}
	
	@Override
	public String toString() {
		return BeanUtil.toString(this);
	}
}
