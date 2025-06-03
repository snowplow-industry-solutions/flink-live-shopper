package com.evoura.snowplow.operator.cart;

import com.evoura.operator.TimestampedEvent;

public class CartEvent implements TimestampedEvent {
  public String eventId;
  public String userId;
  public String type;
  public double previousPrice;
  public double productPrice;
  public long timestamp;

  public CartEvent() {}

  public CartEvent(
      String eventId,
      String userId,
      String type,
      double previousPrice,
      double productPrice,
      long timestamp) {
    this.eventId = eventId;
    this.userId = userId;
    this.type = type;
    this.previousPrice = previousPrice;
    this.productPrice = productPrice;
    this.timestamp = timestamp;
  }

  @Override
  public long getTimestamp() {
    return timestamp;
  }

  @Override
  public String toString() {
    return "CartEvent{"
        + "eventId='"
        + eventId
        + '\''
        + ", userId='"
        + userId
        + '\''
        + ", eventType='"
        + type
        + '\''
        + ", previousPrice="
        + previousPrice
        + ", productPrice="
        + productPrice
        + ", timestamp="
        + timestamp
        + '}';
  }
}
