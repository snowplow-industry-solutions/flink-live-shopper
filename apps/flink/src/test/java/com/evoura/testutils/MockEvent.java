package com.evoura.testutils;

import com.evoura.operator.TimestampedEvent;

public class MockEvent implements TimestampedEvent {
  private final long timestamp;
  private final String value;

  public MockEvent(long timestamp, String value) {
    this.timestamp = timestamp;
    this.value = value;
  }

  @Override
  public long getTimestamp() {
    return timestamp;
  }

  public String getValue() {
    return value;
  }

  @Override
  public String toString() {
    return "MockEvent{" + "timestamp=" + timestamp + ", value='" + value + '\'' + '}';
  }
}
