package com.evoura.snowplow.operator.category;

import com.evoura.snowplow.operator.redis.MetricValue;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.util.Collector;

public class CategoryMetricsGeneratorRichFunction
    extends ProcessFunction<CategoryBehavior, MetricValue> {
  private final long ttlSeconds;

  public CategoryMetricsGeneratorRichFunction(int ttlSeconds) {
    this.ttlSeconds = ttlSeconds;
  }

  @Override
  public void processElement(
      CategoryBehavior categoryBehavior,
      ProcessFunction<CategoryBehavior, MetricValue>.Context context,
      Collector<MetricValue> out) {

    out.collect(
        new MetricValue(
            String.format(
                "user:%s:top_category_%s",
                categoryBehavior.userId, categoryBehavior.windowIdentifier),
            categoryBehavior.mostViewedCategory,
            ttlSeconds));

    out.collect(
        new MetricValue(
            String.format(
                "user:%s:unique_categories_%s",
                categoryBehavior.userId, categoryBehavior.windowIdentifier),
            String.valueOf(categoryBehavior.uniqueCategoryCount),
            ttlSeconds));
  }
}
