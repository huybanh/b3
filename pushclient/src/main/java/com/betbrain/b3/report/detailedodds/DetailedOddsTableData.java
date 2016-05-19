package com.betbrain.b3.report.detailedodds;

import java.util.ArrayList;
import java.util.HashMap;

import javax.xml.bind.annotation.XmlRootElement;

import com.betbrain.b3.api.DetailedOddsTableRowTrait;
import com.betbrain.b3.api.DetailedOddsTableTrait;
import com.betbrain.sepc.connector.util.beans.BeanUtil;

@XmlRootElement
public class DetailedOddsTableData implements DetailedOddsTableTrait {
	
	private String caption;
	
	HashMap<Long, String> oddsProviderNames = new HashMap<>();
	HashMap<Long, String> scoreProviderNames = new HashMap<>();
	HashMap<Long, String> statusProviderNames = new HashMap<>();

	private ArrayList<DetailedOddsTableRowTrait> rows = new ArrayList<>();
	
	void addRow(DetailedOddsTableRow row) {
		rows.add(row);
	}

	/* (non-Javadoc)
	 * @see com.betbrain.b3.report.detailedodds.DetailedOddsTableTrait#getCaption()
	 */
	@Override
	public String getCaption() {
		return caption;
	}

	public void setCaption(String caption) {
		this.caption = caption;
	}

	/* (non-Javadoc)
	 * @see com.betbrain.b3.report.detailedodds.DetailedOddsTableTrait#getOddsProviderNames()
	 */
	@Override
	public HashMap<Long, String> getOddsProviderNames() {
		return oddsProviderNames;
	}

	public void setOddsProviderNames(HashMap<Long, String> oddsProviderNames) {
		this.oddsProviderNames = oddsProviderNames;
	}

	/* (non-Javadoc)
	 * @see com.betbrain.b3.report.detailedodds.DetailedOddsTableTrait#getScoreProviderNames()
	 */
	@Override
	public HashMap<Long, String> getScoreProviderNames() {
		return scoreProviderNames;
	}

	public void setScoreProviderNames(HashMap<Long, String> scoreProviderNames) {
		this.scoreProviderNames = scoreProviderNames;
	}

	/* (non-Javadoc)
	 * @see com.betbrain.b3.report.detailedodds.DetailedOddsTableTrait#getStatusProviderNames()
	 */
	@Override
	public HashMap<Long, String> getStatusProviderNames() {
		return statusProviderNames;
	}

	public void setStatusProviderNames(HashMap<Long, String> statusProviderNames) {
		this.statusProviderNames = statusProviderNames;
	}

	/* (non-Javadoc)
	 * @see com.betbrain.b3.report.detailedodds.DetailedOddsTableTrait#getRows()
	 */
	@Override
	public ArrayList<DetailedOddsTableRowTrait> getRows() {
		return rows;
	}

	public void setRows(ArrayList<DetailedOddsTableRowTrait> rows) {
		this.rows = rows;
	}
	
	@Override
	public String toString() {
		return BeanUtil.toString(this);
	}
}
