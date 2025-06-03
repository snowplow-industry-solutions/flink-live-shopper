package com.evoura.snowplow.operator.cart;

import com.evoura.snowplow.operator.redis.MetricValue;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.util.Collector;

public class CartBehaviorMetricFlatMap implements FlatMapFunction<CartBehavior, MetricValue> {
  public static String IDENTIFIER = "cart-metrics";
  private final long ttlSeconds;

  public CartBehaviorMetricFlatMap(int ttlSeconds) {
    this.ttlSeconds = ttlSeconds;
  }

  @Override
  public void flatMap(CartBehavior cartBehavior, Collector<MetricValue> out) throws Exception {
    out.collect(
        new MetricValue(
            createKey(cartBehavior, "cart_add_count"),
            String.valueOf(cartBehavior.adds),
            ttlSeconds));

    out.collect(
        new MetricValue(
            createKey(cartBehavior, "cart_remove_count"),
            String.valueOf(cartBehavior.removes),
            ttlSeconds));

    out.collect(
        new MetricValue(
            createKey(cartBehavior, "cart_update_frequency"),
            String.valueOf(cartBehavior.adds + cartBehavior.removes),
            ttlSeconds));
  }

  private static String createKey(CartBehavior cartBehavior, String feature) {
    return String.format(
        "user:%s:%s_%s", cartBehavior.userId, feature, cartBehavior.windowIdentifier);
  }
}
