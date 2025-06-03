package com.evoura.snowplow.operator.purchase;

import com.evoura.snowplow.operator.redis.MetricValue;
import java.math.BigDecimal;
import java.math.RoundingMode;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.util.Collector;

public class PurchaseHistoryFlatMap implements FlatMapFunction<PurchaseHistory, MetricValue> {
  public static String IDENTIFIER = "purchase-metrics";
  private final long ttlSeconds;

  public PurchaseHistoryFlatMap(int ttlSeconds) {
    this.ttlSeconds = ttlSeconds;
  }

  @Override
  public void flatMap(PurchaseHistory event, Collector<MetricValue> out) throws Exception {
    int completedPurchases = event.purchases.size();
    double totalValue = event.purchases.values().stream().mapToDouble(Double::doubleValue).sum();

    out.collect(createMetric(event, "orders_count", completedPurchases));

    out.collect(createMetric(event, "order_value", totalValue));

    out.collect(
        createMetric(event, "avg_order_value", getAvgOrderPrice(totalValue, completedPurchases)));
  }

  private MetricValue createMetric(PurchaseHistory event, String avg_order_value, Number value) {
    return new MetricValue(createKey(event, avg_order_value), String.valueOf(value), ttlSeconds);
  }

  private static double getAvgOrderPrice(double totalValue, int completedPurchases) {
    if (completedPurchases == 0) {
      return 0.0;
    }

    BigDecimal total = BigDecimal.valueOf(totalValue);
    BigDecimal divisor = BigDecimal.valueOf(completedPurchases);

    BigDecimal avgOrderValue = total.divide(divisor, RoundingMode.HALF_EVEN);

    return avgOrderValue.setScale(2, RoundingMode.HALF_EVEN).doubleValue();
  }

  private static String createKey(PurchaseHistory event, String feature) {
    return String.format("user:%s:%s_%s", event.userId, feature, event.windowIdentifier);
  }
}
