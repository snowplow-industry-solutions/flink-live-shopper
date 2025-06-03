package com.evoura.snowplow.operator.product;

import com.evoura.operator.TimestampedEvent;

public class ProductViewEvent implements TimestampedEvent {
  public String eventId;
  public String userId;
  public String productId;
  public double productPrice;
  public long timestamp;

  public ProductViewEvent() {}

  public ProductViewEvent(
      String eventId, String userId, String productId, double productPrice, long timestamp) {
    this.eventId = eventId;
    this.userId = userId;
    this.productId = productId;
    this.productPrice = productPrice;
    this.timestamp = timestamp;
  }

  @Override
  public long getTimestamp() {
    return timestamp;
  }

  @Override
  public String toString() {
    return "ProductViewEvent{"
        + "eventId='"
        + eventId
        + '\''
        + ", userId='"
        + userId
        + '\''
        + ", productId='"
        + productId
        + '\''
        + ", productPrice="
        + productPrice
        + '}';
  }
}
