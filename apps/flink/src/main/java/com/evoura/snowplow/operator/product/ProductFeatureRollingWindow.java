package com.evoura.snowplow.operator.product;

import com.evoura.operator.RollingWindowProcessFunction;
import java.time.Duration;
import java.util.DoubleSummaryStatistics;
import java.util.Map;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.StreamSupport;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.typeutils.ListTypeInfo;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;

public class ProductFeatureRollingWindow
    extends RollingWindowProcessFunction<String, ProductViewEvent, ProductFeature> {
  public static final String IDENTIFIER = "product-feature-rolling-window";

  private static final long serialVersionUID = -1507218873473876883L;

  private static final TypeInformation<ProductViewEvent> TYPE_INFO =
      ListTypeInfo.of(ProductViewEvent.class);

  private final String windowIdentifier;

  public ProductFeatureRollingWindow(
      Duration windowSize, Duration emitInterval, String windowIdentifier) {
    super(windowSize, emitInterval, TYPE_INFO);

    this.windowIdentifier = windowIdentifier;
  }

  @Override
  public void processWindow(
      Iterable<ProductViewEvent> events,
      KeyedProcessFunction<String, ProductViewEvent, ProductFeature>.OnTimerContext ctx,
      Collector<ProductFeature> out)
      throws Exception {
    DoubleSummaryStatistics priceStats =
        StreamSupport.stream(events.spliterator(), false)
            .collect(
                Collectors.summarizingDouble(productViewEvent -> productViewEvent.productPrice));

    Map<String, ProductViewEvent> uniqueEvents =
        StreamSupport.stream(events.spliterator(), false)
            .collect(
                Collectors.toMap(
                    productViewEvent -> productViewEvent.productId,
                    Function.identity(),
                    (existing, replacement) -> existing));

    int uniqueViewsCount = uniqueEvents.size();

    double avgUniquePrice =
        uniqueEvents.values().stream()
            .mapToDouble(productViewEvent -> productViewEvent.productPrice)
            .average()
            .orElse(0.0);

    Map<String, Long> productViewCounts =
        StreamSupport.stream(events.spliterator(), false)
            .collect(
                Collectors.groupingBy(
                    productViewEvent -> productViewEvent.productId, Collectors.counting()));

    out.collect(
        new ProductFeature(
            ctx.getCurrentKey(),
            uniqueViewsCount,
            avgUniquePrice,
            uniqueViewsCount == 0 ? 0.0 : priceStats.getMin(),
            uniqueViewsCount == 0 ? 0.0 : priceStats.getMax(),
            windowIdentifier,
            productViewCounts));
  }
}
