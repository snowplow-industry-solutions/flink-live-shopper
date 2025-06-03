package com.evoura.snowplow.operator.cart;

import com.evoura.operator.RollingWindowProcessFunction;
import java.time.Duration;
import java.util.Map;
import java.util.stream.Collectors;
import java.util.stream.StreamSupport;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.typeutils.ListTypeInfo;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;

public class CartBehaviorRollingWindow
    extends RollingWindowProcessFunction<String, CartEvent, CartBehavior> {
  public static final String IDENTIFIER = "cart-behavior-rolling-window";

  private static final long serialVersionUID = -1507218873473876883L;

  private static final TypeInformation<CartEvent> TYPE_INFO = ListTypeInfo.of(CartEvent.class);

  private final String windowIdentifier;

  public CartBehaviorRollingWindow(
      Duration windowSize, Duration emitInterval, String windowIdentifier) {
    super(windowSize, emitInterval, TYPE_INFO);

    this.windowIdentifier = windowIdentifier;
  }

  @Override
  public void processWindow(
      Iterable<CartEvent> events,
      KeyedProcessFunction<String, CartEvent, CartBehavior>.OnTimerContext ctx,
      Collector<CartBehavior> out)
      throws Exception {
    Map<String, Long> count =
        StreamSupport.stream(events.spliterator(), false)
            .collect(Collectors.groupingBy(cartEvent -> cartEvent.type, Collectors.counting()));

    long adds = count.getOrDefault("add_to_cart", 0L);
    long removes = count.getOrDefault("remove_from_cart", 0L);

    out.collect(new CartBehavior(windowIdentifier, ctx.getCurrentKey(), adds, removes));
  }
}
