package com.evoura.operator;

import java.io.Serializable;

/**
 * Represents an event that carries an intrinsic timestamp, crucial for event time processing in
 * Flink.
 *
 * <p>This interface must be implemented by all event types intended for use with Flink operators
 * that rely on event time, such as time-based windowing or joining. The timestamp returned by
 * {@link #getTimestamp()} is fundamental for:
 *
 * <ul>
 *   <li>Assigning events to time windows.
 *   <li>Generating watermarks to track the progress of event time.
 *   <li>Ordering events for processing based on their occurrence time.
 * </ul>
 *
 * <p>Implementations must ensure that the timestamp is consistently represented, typically as epoch
 * milliseconds. The interface extends {@link Serializable} because event objects are often
 * serialized and sent across the network in distributed Flink deployments.
 *
 * <p>Note: Flink now primarily uses event time semantics by default, making timestamp assignment
 * via interfaces like this or through dedicated {@code TimestampAssigner}s essential. The older
 * {@code TimeCharacteristic} configuration is deprecated.
 */
public interface TimestampedEvent extends Serializable {

  /**
   * Returns the timestamp of this event.
   *
   * <p>The timestamp value is used for time-based processing such as windowing and watermarking.
   *
   * @return the event's timestamp (e.g., in epoch milliseconds)
   */
  long getTimestamp();
}
