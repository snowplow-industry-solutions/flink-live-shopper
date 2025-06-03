package com.evoura.snowplow.operator.product;

import com.evoura.snowplow.operator.redis.MetricValue;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.util.Collector;

public class ProductMetricsGeneratorRichFunction
    extends ProcessFunction<ProductFeature, MetricValue> {

  private final long ttlSeconds;

  public ProductMetricsGeneratorRichFunction(int ttlSeconds) {
    this.ttlSeconds = ttlSeconds;
  }

  @Override
  public void processElement(
      ProductFeature productFeature,
      ProcessFunction<ProductFeature, MetricValue>.Context context,
      Collector<MetricValue> out) {

    out.collect(
        new MetricValue(
            String.format(
                "user:%s:product_view_count_%s", productFeature.userId, productFeature.windowSize),
            String.valueOf(productFeature.uniqueProductCount),
            ttlSeconds));

    out.collect(
        new MetricValue(
            String.format(
                "user:%s:avg_viewed_price_%s", productFeature.userId, productFeature.windowSize),
            String.valueOf(productFeature.averagePrice),
            ttlSeconds));

    out.collect(
        new MetricValue(
            String.format(
                "user:%s:return_view_count_%s", productFeature.userId, productFeature.windowSize),
            String.valueOf(productFeature.views),
            ttlSeconds));

    out.collect(
        new MetricValue(
            String.format(
                "user:%s:price_range_viewed_%s", productFeature.userId, productFeature.windowSize),
            productFeature.minPrice + "-" + productFeature.maxPrice,
            ttlSeconds));
  }
}
