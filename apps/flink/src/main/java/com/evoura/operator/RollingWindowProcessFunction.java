package com.evoura.operator;

import java.io.IOException;
import java.time.Duration;
import java.time.Instant;
import java.util.Comparator;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.StreamSupport;
import org.apache.flink.api.common.functions.OpenContext;
import org.apache.flink.api.common.state.ListState;
import org.apache.flink.api.common.state.ListStateDescriptor;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * A base class for implementing rolling window logic using Flink's {@link KeyedProcessFunction}.
 *
 * <p>This function maintains events within a specified rolling window duration based on event time.
 * It periodically triggers processing of the events currently in the window based on a defined emit
 * interval. The window slides forward with the event time watermark.
 *
 * <p>State Management:
 *
 * <ul>
 *   <li>{@code eventsState}: A {@link ListState} storing events that fall within the current
 *       window.
 *   <li>{@code nextEmitTime}: A {@link ValueState} tracking the timestamp for the next scheduled
 *       timer trigger.
 * </ul>
 *
 * <p>Processing Logic:
 *
 * <ol>
 *   <li>Incoming events are added to {@code eventsState} in {@link #processElement(EVENT, Context,
 *       Collector)}.
 *   <li>A timer is scheduled based on the {@code emitInterval} and the current watermark.
 *   <li>When the timer fires ({@link #onTimer(long, OnTimerContext, Collector)}), events older than
 *       {@code (watermark - windowSize)} are pruned from {@code eventsState}.
 *   <li>The remaining events (sorted by timestamp) are passed to the abstract {@link
 *       #processWindow(Iterable, OnTimerContext, Collector)} method for custom processing.
 *   <li>A new timer is scheduled for the next emit interval.
 * </ol>
 *
 * @param <KEY> The type of the key by which the stream is partitioned.
 * @param <EVENT> The type of the input events. Must extend {@link TimestampedEvent}.
 * @param <OUTPUT> The type of the output elements.
 */
public abstract class RollingWindowProcessFunction<KEY, EVENT extends TimestampedEvent, OUTPUT>
    extends KeyedProcessFunction<KEY, EVENT, OUTPUT> {
  private static final Logger LOG = LoggerFactory.getLogger(RollingWindowProcessFunction.class);
  private static final long serialVersionUID = -1507218873473876883L;

  private final Duration windowSize;
  private final Duration emitInterval;
  private final TypeInformation<EVENT> eventTypeInfo;

  private transient ListState<EVENT> eventsState;
  private transient ValueState<Long> nextEmitTime;

  public RollingWindowProcessFunction(
      Duration windowSize, Duration emitInterval, TypeInformation<EVENT> eventTypeInfo) {
    this.windowSize = windowSize;
    this.emitInterval = emitInterval;
    this.eventTypeInfo = eventTypeInfo;
  }

  /**
   * Processes the events within the current window for the current key.
   *
   * <p>This method is invoked by {@link #onTimer(long, OnTimerContext, Collector)} when a scheduled
   * timer fires. It receives all events currently held in state that fall within the active window,
   * defined as the time range {@code (watermark - windowSize, watermark]}. The provided events are
   * guaranteed to be sorted by their timestamps in ascending order.
   *
   * <p>Implementations should define their specific window processing logic here, such as
   * aggregation, transformation, or filtering. If no events are present in the window when the
   * timer fires, this method is still called with an empty {@code Iterable}.
   *
   * @param events An iterable collection of events currently within the window, sorted by
   *     timestamp. This iterable might be empty if no events arrived for the window.
   * @param ctx The context associated with the timer invocation, providing access to the current
   *     key, timer service, time information, and state.
   * @param out A collector for emitting resulting output elements.
   * @throws Exception Implementations may throw exceptions, which will cause task failure and
   *     potential recovery.
   */
  public abstract void processWindow(
      Iterable<EVENT> events,
      KeyedProcessFunction<KEY, EVENT, OUTPUT>.OnTimerContext ctx,
      Collector<OUTPUT> out)
      throws Exception;

  @Override
  public void open(OpenContext openContext) throws Exception {
    super.open(openContext);

    eventsState =
        getRuntimeContext().getListState(new ListStateDescriptor<>("events-state", eventTypeInfo));

    nextEmitTime =
        getRuntimeContext().getState(new ValueStateDescriptor<>("next-emit-time", Long.class));
  }

  @Override
  public void processElement(EVENT event, Context ctx, Collector<OUTPUT> out) throws Exception {
    eventsState.add(event);

    LOG.debug(
        "Event from key {} added to window - Timestamp: {}",
        ctx.getCurrentKey(),
        Instant.ofEpochMilli(event.getTimestamp()));

    scheduleEmitTimer(ctx);
  }

  @Override
  public void onTimer(long timestamp, OnTimerContext ctx, Collector<OUTPUT> out) throws Exception {
    LOG.debug("Timer fired at: {}", Instant.ofEpochMilli(timestamp));

    cleanupAndSortEvents(ctx);

    // If there are no events in the state, the timer is not scheduled, and state is cleaned up.
    Iterable<EVENT> events = eventsState.get();
    if (events == null || !events.iterator().hasNext()) {
      eventsState.clear();
      nextEmitTime.clear();

      processWindow(List.of(), ctx, out);
      return;
    }

    processWindow(events, ctx, out);
    scheduleEmitTimer(ctx);
  }

  private void cleanupAndSortEvents(KeyedProcessFunction<KEY, EVENT, OUTPUT>.OnTimerContext ctx)
      throws Exception {
    long watermark = ctx.timerService().currentWatermark();

    long minWindowTimestamp = watermark - windowSize.toMillis();

    LOG.debug("Processing: {}", Instant.ofEpochMilli(ctx.timerService().currentProcessingTime()));
    LOG.debug("Watermark : {}", Instant.ofEpochMilli(watermark));
    LOG.debug("Min Window: {}", Instant.ofEpochMilli(minWindowTimestamp));

    Iterable<EVENT> events = eventsState.get();
    List<EVENT> filteredEvents =
        StreamSupport.stream(events.spliterator(), true)
            .filter(event -> event.getTimestamp() >= minWindowTimestamp)
            .sorted(
                Comparator.comparingLong(
                    EVENT::getTimestamp)) // Added due to Snowplow unordered events
            .collect(Collectors.toList());

    eventsState.update(filteredEvents);
  }

  private void scheduleEmitTimer(KeyedProcessFunction<KEY, EVENT, OUTPUT>.Context ctx)
      throws IOException {
    long currentTime = ctx.timerService().currentWatermark();

    if (nextEmitTime.value() == null) {
      nextEmitTime.update(currentTime + 1);
    } else {
      nextEmitTime.update(currentTime + emitInterval.toMillis());
    }

    ctx.timerService().registerEventTimeTimer(nextEmitTime.value());
  }
}
