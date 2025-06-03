package com.evoura.snowplow.operator.session;

import com.evoura.operator.SessionWindowProcessFunction;
import com.evoura.snowplow.model.SnowplowEvent;
import com.evoura.snowplow.operator.StaticContext;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import java.time.Duration;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.StreamSupport;
import org.apache.flink.api.java.typeutils.ListTypeInfo;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class SnowplowSessionWindow
    extends SessionWindowProcessFunction<String, SnowplowEvent, SessionFeature> {
  private static final Logger LOG = LoggerFactory.getLogger(SnowplowSessionWindow.class);

  public SnowplowSessionWindow(Duration emitInterval, Duration maxGap) {
    super(emitInterval, maxGap, ListTypeInfo.of(SnowplowEvent.class));
  }

  @Override
  public void processWindow(
      Iterable<SnowplowEvent> events,
      KeyedProcessFunction<String, SnowplowEvent, SessionFeature>.OnTimerContext ctx,
      Collector<SessionFeature> out)
      throws Exception {

    if (getWindowStartTime() == null) {
      LOG.error("Window start time is null");
      return;
    }

    long duration = Math.max(ctx.timerService().currentWatermark() - getWindowStartTime(), 0L);

    // Filter all page_view type and count it
    long pageViewCount = countEventType(events, "page_view");

    // Filter all page_view type, check if payload `page_url` has search query and count it
    long searchCount =
        StreamSupport.stream(events.spliterator(), false)
            .filter(event -> event.getType().equals("page_view"))
            .filter(SnowplowSessionWindow::hasSearchQuery)
            .count();

    // Filter all product_view type and count it
    long productViewCount = countEventType(events, "product_view");

    // Filter all add_to_cart type and count it
    long cartAddCount = countEventType(events, "add_to_cart");

    // Get last event user_id
    String userId =
        StreamSupport.stream(events.spliterator(), false)
            .reduce((first, second) -> second)
            .map(SnowplowEvent::getUserId)
            .orElse(null);

    // Get last event enrichment paltform value
    String lastPlatform =
        StreamSupport.stream(events.spliterator(), false)
            .reduce((first, second) -> second)
            .map(snowplowEvent -> snowplowEvent.enrichment.platform)
            .orElse(null);

    // Find first event with a non-null enrichment marketing campaign and get the campaign value
    String campaign =
        StreamSupport.stream(events.spliterator(), false)
            .filter(snowplowEvent -> snowplowEvent.enrichment.marketing != null)
            .findFirst()
            .map(snowplowEvent -> snowplowEvent.enrichment.marketing.campaign)
            .orElse(null);

    // Get a list of deviceTypes from the events
    List<String> deviceTypes =
        StreamSupport.stream(events.spliterator(), false)
            .map(snowplowEvent -> snowplowEvent.userAgent.platform)
            .distinct()
            .collect(Collectors.toList());

    out.collect(
        new SessionFeature(
            getWindowStartTime(),
            ctx.getCurrentKey(),
            duration,
            userId,
            pageViewCount,
            searchCount,
            productViewCount,
            cartAddCount,
            campaign,
            deviceTypes,
            lastPlatform));
  }

  private static long countEventType(Iterable<SnowplowEvent> events, String product_view) {
    return StreamSupport.stream(events.spliterator(), false)
        .filter(event -> event.getType().equals(product_view))
        .count();
  }

  private static boolean hasSearchQuery(SnowplowEvent event) {
    try {
      JsonNode payload = StaticContext.MAPPER.readTree(event.payload);

      return payload.has("page_url")
          && payload.get("page_url").isTextual()
          && payload.get("page_url").asText().contains("/search?q=");
    } catch (JsonProcessingException e) {
      throw new RuntimeException(e);
    }
  }
}
