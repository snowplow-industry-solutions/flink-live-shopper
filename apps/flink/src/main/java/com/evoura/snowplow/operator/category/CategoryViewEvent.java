package com.evoura.snowplow.operator.category;

import com.evoura.operator.TimestampedEvent;

public class CategoryViewEvent implements TimestampedEvent {
  public String eventId;
  public String userId;
  public String category;
  public long timestamp;

  public CategoryViewEvent() {}

  public CategoryViewEvent(String eventId, String userId, String category, long timestamp) {
    this.eventId = eventId;
    this.userId = userId;
    this.category = category;
    this.timestamp = timestamp;
  }

  @Override
  public long getTimestamp() {
    return timestamp;
  }

  @Override
  public String toString() {
    return "CategoryViewEvent{"
        + "eventId='"
        + eventId
        + '\''
        + ", userId='"
        + userId
        + '\''
        + ", category='"
        + category
        + '\''
        + ", timestamp="
        + timestamp
        + '}';
  }
}
