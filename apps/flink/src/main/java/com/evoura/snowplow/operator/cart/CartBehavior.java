package com.evoura.snowplow.operator.cart;

import java.io.Serializable;

public class CartBehavior implements Serializable {
  public String windowIdentifier;
  public String userId;
  public long adds;
  public long removes;

  public CartBehavior() {}

  public CartBehavior(String windowIdentifier, String userId, long adds, long removes) {
    this.windowIdentifier = windowIdentifier;
    this.userId = userId;
    this.adds = adds;
    this.removes = removes;
  }

  @Override
  public String toString() {
    return "CartBehavior{"
        + "windowIdentifier='"
        + windowIdentifier
        + '\''
        + ", userId='"
        + userId
        + '\''
        + ", adds="
        + adds
        + ", removes="
        + removes
        + '}';
  }
}
