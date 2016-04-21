package com.betbrain.b3.pushclient;

import java.util.Date;

/**
 * See the Sports Model documentation.
 * 
 * @author Morten Helles
 */
public class Event extends Entity {
    
    // --------------------------------------------------------------------------------------------
    // Class fields
    // --------------------------------------------------------------------------------------------
    
    private static final long serialVersionUID = 1L;
    
    public static final String PROPERTY_NAME_isComplete = "isComplete";
    public static final String PROPERTY_NAME_sportId = "sportId";
    public static final String PROPERTY_NAME_templateId = "templateId";
    public static final String PROPERTY_NAME_promotionId = "promotionId";
    public static final String PROPERTY_NAME_parentId = "parentId";
    public static final String PROPERTY_NAME_parentPartId = "parentPartId";
    public static final String PROPERTY_NAME_name = "name";
    public static final String PROPERTY_NAME_startTime = "startTime";
    public static final String PROPERTY_NAME_endTime = "endTime";
    public static final String PROPERTY_NAME_deleteTimeOffset = "deleteTimeOffset";
    public static final String PROPERTY_NAME_venueId = "venueId";
    public static final String PROPERTY_NAME_statusId = "statusId";
    public static final String PROPERTY_NAME_hasLiveStatus = "hasLiveStatus";
    public static final String PROPERTY_NAME_rootPartId = "rootPartId";
    public static final String PROPERTY_NAME_currentPartId = "currentPartId";
    public static final String PROPERTY_NAME_url = "url";
    public static final String PROPERTY_NAME_popularity = "popularity";
    public static final String PROPERTY_NAME_note = "note";
    
    // --------------------------------------------------------------------------------------------
    // Instance fields
    // --------------------------------------------------------------------------------------------
    
    private boolean _isComplete;
    private long _sportId;
    private Long _templateId;
    private Long _promotionId;
    private Long _parentId;
    private Long _parentPartId;
    private String _name;
    private Date _startTime;
    private Date _endTime;
    private long _deleteTimeOffset;
    private Long _venueId;
    private long _statusId;
    private boolean _hasLiveStatus;
    private long _rootPartId;
    private Long _currentPartId;
    private String _url;
    private Integer _popularity;
    private String _note;
    
    // --------------------------------------------------------------------------------------------
    // Constructors
    // --------------------------------------------------------------------------------------------
    
    public Event() {
    
    }
    
    // --------------------------------------------------------------------------------------------
    // Instance methods
    // --------------------------------------------------------------------------------------------
    
    public boolean getIsComplete() {
    
        return _isComplete;
    }
    
    /* ----------------------------------------------------------------------------------------- */
    
    public void setIsComplete(boolean isComplete) {
    
        _isComplete = isComplete;
    }
    
    /* ----------------------------------------------------------------------------------------- */
    
    public long getSportId() {
    
        return _sportId;
    }
    
    /* ----------------------------------------------------------------------------------------- */
    
    public void setSportId(long sportId) {
    
        _sportId = sportId;
    }
    
    /* ----------------------------------------------------------------------------------------- */
    
    public Long getTemplateId() {
    
        return _templateId;
    }
    
    /* ----------------------------------------------------------------------------------------- */
    
    public void setTemplateId(Long templateId) {
    
        _templateId = templateId;
    }
    
    /* ----------------------------------------------------------------------------------------- */
    
    public Long getPromotionId() {
    
        return _promotionId;
    }
    
    /* ----------------------------------------------------------------------------------------- */
    
    public void setPromotionId(Long promotionId) {
    
        _promotionId = promotionId;
    }
    
    /* ----------------------------------------------------------------------------------------- */
    
    public Long getParentId() {
    
        return _parentId;
    }
    
    /* ----------------------------------------------------------------------------------------- */
    
    public void setParentId(Long parentId) {
    
        _parentId = parentId;
    }
    
    /* ----------------------------------------------------------------------------------------- */
    
    public Long getParentPartId() {
    
        return _parentPartId;
    }
    
    /* ----------------------------------------------------------------------------------------- */
    
    public void setParentPartId(Long parentPartId) {
    
        _parentPartId = parentPartId;
    }
    
    /* ----------------------------------------------------------------------------------------- */
    
    public String getName() {
    
        return _name;
    }
    
    /* ----------------------------------------------------------------------------------------- */
    
    public void setName(String name) {
    
        _name = name;
    }
    
    /* ----------------------------------------------------------------------------------------- */
    
    public Date getStartTime() {
    
        return _startTime;
    }
    
    /* ----------------------------------------------------------------------------------------- */
    
    public void setStartTime(Date startTime) {
    
        _startTime = startTime;
    }
    
    /* ----------------------------------------------------------------------------------------- */
    
    public Date getEndTime() {
    
        return _endTime;
    }
    
    /* ----------------------------------------------------------------------------------------- */
    
    public void setEndTime(Date endTime) {
    
        _endTime = endTime;
    }
    
    /* ----------------------------------------------------------------------------------------- */
    
    public long getDeleteTimeOffset() {
    
        return _deleteTimeOffset;
    }
    
    /* ----------------------------------------------------------------------------------------- */
    
    public void setDeleteTimeOffset(long deleteTimeOffset) {
    
        _deleteTimeOffset = deleteTimeOffset;
    }
    
    /* ----------------------------------------------------------------------------------------- */
    
    public Long getVenueId() {
    
        return _venueId;
    }
    
    /* ----------------------------------------------------------------------------------------- */
    
    public void setVenueId(Long venueId) {
    
        _venueId = venueId;
    }
    
    /* ----------------------------------------------------------------------------------------- */
    
    public long getStatusId() {
    
        return _statusId;
    }
    
    /* ----------------------------------------------------------------------------------------- */
    
    public void setStatusId(long statusId) {
    
        _statusId = statusId;
    }
    
    /* ----------------------------------------------------------------------------------------- */
    
    public boolean getHasLiveStatus() {
    
        return _hasLiveStatus;
    }

    /* ----------------------------------------------------------------------------------------- */
    
    public void setHasLiveStatus(boolean hasLiveStatus) {
    
        _hasLiveStatus = hasLiveStatus;
    }
    
    /* ----------------------------------------------------------------------------------------- */
    
    public long getRootPartId() {
    
        return _rootPartId;
    }
    
    /* ----------------------------------------------------------------------------------------- */
    
    public void setRootPartId(long rootPartId) {
    
        _rootPartId = rootPartId;
    }
    
    /* ----------------------------------------------------------------------------------------- */
    
    public Long getCurrentPartId() {
    
        return _currentPartId;
    }
    
    /* ----------------------------------------------------------------------------------------- */
    
    public void setCurrentPartId(Long currentPartId) {
    
        _currentPartId = currentPartId;
    }
    
    /* ----------------------------------------------------------------------------------------- */
    
    public String getUrl() {
    
        return _url;
    }
    
    /* ----------------------------------------------------------------------------------------- */
    
    public void setUrl(String url) {
    
        _url = url;
    }
    
    /* ----------------------------------------------------------------------------------------- */
    
    public Integer getPopularity() {
    
        return _popularity;
    }
    
    /* ----------------------------------------------------------------------------------------- */
    
    public void setPopularity(Integer popularity) {
    
        _popularity = popularity;
    }
    
    /* ----------------------------------------------------------------------------------------- */
    
    public String getNote() {
    
        return _note;
    }
    
    /* ----------------------------------------------------------------------------------------- */
    
    public void setNote(String note) {
    
        _note = note;
    }
    
    /* ----------------------------------------------------------------------------------------- */
}