package com.evoura.operator;

import java.io.IOException;
import java.time.Duration;
import java.time.Instant;
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
 * A base class for implementing session window logic using Flink's {@link KeyedProcessFunction}.
 *
 * <p>This function groups events into sessions based on event time activity. A session is defined
 * as a period of activity where the time gap between consecutive events does not exceed a specified
 * {@code maxGap}. The function emits results periodically based on a defined {@code emitInterval}
 * or when a session window closes due to inactivity.
 *
 * <p>State Management:
 *
 * <ul>
 *   <li>{@code eventsState}: A {@link ListState} storing events belonging to the current active
 *       session window.
 *   <li>{@code windowStartTime}: A {@link ValueState} holding the timestamp of the first event in
 *       the current session.
 *   <li>{@code lastProcessedTime}: A {@link ValueState} tracking the timestamp of the most recently
 *       processed event.
 *   <li>{@code nextEmitTime}: A {@link ValueState} tracking the timestamp for the next scheduled
 *       periodic emit timer.
 *   <li>{@code windowClearTime}: A {@link ValueState} holding the timestamp when the current
 *       session window is scheduled to be cleared due to inactivity (based on {@code maxGap}).
 * </ul>
 *
 * <p>Processing Logic:
 *
 * <ol>
 *   <li>Incoming events in {@link #processElement(EVENT, Context, Collector)} are added to {@code
 *       eventsState}.
 *   <li>The {@code windowStartTime} is set if it's the first event of the session.
 *   <li>A {@code windowClearTime} timer is registered based on the event's timestamp plus the
 *       {@code maxGap}. Any existing clear timer is deleted.
 *   <li>A periodic emit timer is scheduled or rescheduled via {@link #scheduleNextEmitter(Context)}
 *       based on the {@code emitInterval}.
 *   <li>When any timer fires ({@link #onTimer(long, OnTimerContext, Collector)}), the current
 *       contents of {@code eventsState} are passed to the abstract {@link #processWindow(Iterable,
 *       OnTimerContext, Collector)} method for custom processing.
 *   <li>If the timer corresponds to the {@code windowClearTime}, the session ends, and all related
 *       state is cleared via {@link #clearCurrentState(OnTimerContext)}.
 *   <li>Otherwise (if it's a periodic emit timer), a new emit timer is scheduled.
 * </ol>
 *
 * @param <KEY> The type of the key by which the stream is partitioned.
 * @param <EVENT> The type of the input events. Must extend {@link TimestampedEvent}.
 * @param <OUTPUT> The type of the output elements.
 */
public abstract class SessionWindowProcessFunction<KEY, EVENT extends TimestampedEvent, OUTPUT>
    extends KeyedProcessFunction<KEY, EVENT, OUTPUT> {
  private static final Logger LOG = LoggerFactory.getLogger(SessionWindowProcessFunction.class);
  private static final long serialVersionUID = -3699712445919442315L;

  private final Duration emitInterval;
  private final Duration maxGap;
  private final TypeInformation<EVENT> eventTypeInfo;

  private transient ValueState<Long> windowStartTime;
  private transient ValueState<Long> lastProcessedTime;
  private transient ListState<EVENT> eventsState;
  private transient ValueState<Long> nextEmitTime;
  private transient ValueState<Long> windowClearTime;

  public SessionWindowProcessFunction(
      Duration emitInterval, Duration maxGap, TypeInformation<EVENT> eventTypeInfo) {
    this.emitInterval = emitInterval;
    this.maxGap = maxGap;
    this.eventTypeInfo = eventTypeInfo;
  }

  /**
   * Processes the events collected within the session window up to the point the timer fired.
   *
   * <p>This method is invoked by {@link #onTimer(long, OnTimerContext, Collector)} when either a
   * periodic emit timer or the session-closing timer fires. It receives all events currently held
   * in the {@code eventsState} for the active session. The order of events within the iterable is
   * not guaranteed unless explicitly sorted beforehand.
   *
   * <p>Implementations should define their specific window processing logic here, such as
   * aggregation, analysis, or transformation based on the collected session events. If no events
   * are present in the state when the timer fires, this method might still be called with an empty
   * {@code Iterable}.
   *
   * @param events An iterable collection of events accumulated in the session window so far. This
   *     iterable might be empty.
   * @param ctx The context associated with the timer invocation, providing access to the current
   *     key, timer service, time information, and state. Useful for accessing session metadata like
   *     {@link #getWindowStartTime()}.
   * @param out A collector for emitting resulting output elements based on the session processing.
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

    windowStartTime =
        getRuntimeContext().getState(new ValueStateDescriptor<>("window-start-time", Long.class));
    lastProcessedTime =
        getRuntimeContext().getState(new ValueStateDescriptor<>("last-processed-time", Long.class));

    nextEmitTime =
        getRuntimeContext().getState(new ValueStateDescriptor<>("next-emit-time", Long.class));

    windowClearTime =
        getRuntimeContext().getState(new ValueStateDescriptor<>("window-clear-time", Long.class));
  }

  @Override
  public void processElement(EVENT event, Context ctx, Collector<OUTPUT> out) throws Exception {
    lastProcessedTime.update(event.getTimestamp());

    // Set the window start (only on the first event of the session)
    if (windowStartTime.value() == null) {
      windowStartTime.update(event.getTimestamp());
    }

    // If there is already a clear timer scheduled, delete it
    if (windowClearTime.value() != null) {
      ctx.timerService().deleteEventTimeTimer(windowClearTime.value());
    }

    // Schedule the clear timer
    long clearTime = event.getTimestamp() + maxGap.toMillis();
    windowClearTime.update(clearTime);
    ctx.timerService().registerEventTimeTimer(clearTime);

    // Add event to the window
    eventsState.add(event);

    LOG.debug(
        "Event from key {} added to window - Timestamp: {}",
        ctx.getCurrentKey(),
        Instant.ofEpochMilli(event.getTimestamp()));

    scheduleNextEmitter(ctx);
  }

  public final Long getWindowStartTime() throws Exception {
    return windowStartTime.value();
  }

  @Override
  public void onTimer(long timestamp, OnTimerContext ctx, Collector<OUTPUT> out) throws Exception {
    LOG.debug("Timer fired at: {}", Instant.ofEpochMilli(timestamp));

    processWindow(eventsState.get(), ctx, out);

    if (windowClearTime.value() == null || timestamp == windowClearTime.value()) {
      clearCurrentState(ctx);
    } else {
      scheduleNextEmitter(ctx);
    }
  }

  private void scheduleNextEmitter(KeyedProcessFunction<KEY, EVENT, OUTPUT>.Context ctx)
      throws IOException {
    long currentTime = ctx.timerService().currentWatermark();

    if (nextEmitTime.value() == null) {
      nextEmitTime.update(currentTime + 1);
    } else {
      nextEmitTime.update(currentTime + emitInterval.toMillis());
    }

    ctx.timerService().registerEventTimeTimer(nextEmitTime.value());
  }

  private void clearCurrentState(KeyedProcessFunction<KEY, EVENT, OUTPUT>.OnTimerContext ctx)
      throws IOException {
    LOG.debug("Clearing state for key: {}", ctx.getCurrentKey());

    windowStartTime.clear();
    lastProcessedTime.clear();
    windowClearTime.clear();

    ctx.timerService().deleteEventTimeTimer(nextEmitTime.value());
    nextEmitTime.clear();
  }
}
