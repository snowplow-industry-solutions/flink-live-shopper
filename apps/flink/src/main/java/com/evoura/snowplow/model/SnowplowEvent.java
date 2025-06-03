package com.evoura.snowplow.model;

import com.evoura.operator.TimestampedEvent;

public class SnowplowEvent implements TimestampedEvent {
  public String eventId;
  public String type;
  public String sessionId;
  public String userId;
  public String payload;
  public EventUserAgent userAgent;
  public Enrichment enrichment;
  public long timestamp;

  public SnowplowEvent() {}

  public SnowplowEvent(
      String eventId,
      String type,
      String sessionId,
      String userId,
      String payload,
      EventUserAgent userAgent,
      Enrichment enrichment,
      long timestamp) {
    this.eventId = eventId;
    this.type = type;
    this.sessionId = sessionId;
    this.userId = userId;
    this.payload = payload;
    this.userAgent = userAgent;
    this.enrichment = enrichment;
    this.timestamp = timestamp;
  }

  public String getEventId() {
    return eventId;
  }

  public String getType() {
    return type;
  }

  public String getSessionId() {
    return sessionId;
  }

  public String getUserId() {
    return userId;
  }

  public long getTimestamp() {
    return timestamp;
  }

  @Override
  public String toString() {
    return "SnowplowEvent{"
        + "eventId='"
        + eventId
        + '\''
        + ", type='"
        + type
        + '\''
        + ", userId='"
        + userId
        + '\''
        + ", userAgent="
        + userAgent
        + ", timestamp="
        + timestamp
        + '}';
  }
}
