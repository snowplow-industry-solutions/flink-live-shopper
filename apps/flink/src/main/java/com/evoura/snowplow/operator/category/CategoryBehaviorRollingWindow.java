package com.evoura.snowplow.operator.category;

import com.evoura.operator.RollingWindowProcessFunction;
import java.time.Duration;
import java.util.Map;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.StreamSupport;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.typeutils.ListTypeInfo;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;

public class CategoryBehaviorRollingWindow
    extends RollingWindowProcessFunction<String, CategoryViewEvent, CategoryBehavior> {
  public static final String IDENTIFIER = "category-behavior-rolling-window";

  private static final long serialVersionUID = -1507218873473876883L;

  private static final TypeInformation<CategoryViewEvent> TYPE_INFO =
      ListTypeInfo.of(CategoryViewEvent.class);

  private final String windowIdentifier;

  public CategoryBehaviorRollingWindow(
      Duration windowSize, Duration emitInterval, String windowIdentifier) {
    super(windowSize, emitInterval, TYPE_INFO);

    this.windowIdentifier = windowIdentifier;
  }

  @Override
  public void processWindow(
      Iterable<CategoryViewEvent> events,
      KeyedProcessFunction<String, CategoryViewEvent, CategoryBehavior>.OnTimerContext ctx,
      Collector<CategoryBehavior> out)
      throws Exception {
    Map<String, CategoryViewEvent> uniqueEvents =
        StreamSupport.stream(events.spliterator(), false)
            .collect(
                Collectors.toMap(
                    productViewEvent -> productViewEvent.category,
                    Function.identity(),
                    (existing, replacement) -> existing));

    int uniqueViewsCount = uniqueEvents.size();

    Map<String, Long> categoryViewCounts =
        StreamSupport.stream(events.spliterator(), false)
            .collect(
                Collectors.groupingBy(
                    productViewEvent -> productViewEvent.category, Collectors.counting()));

    // Get the most viewed category
    String mostViewedCategory =
        categoryViewCounts.entrySet().stream()
            .max(Map.Entry.comparingByValue())
            .map(Map.Entry::getKey)
            .orElse(null);

    out.collect(
        new CategoryBehavior(
            windowIdentifier,
            ctx.getCurrentKey(),
            uniqueViewsCount,
            mostViewedCategory,
            categoryViewCounts));
  }
}
