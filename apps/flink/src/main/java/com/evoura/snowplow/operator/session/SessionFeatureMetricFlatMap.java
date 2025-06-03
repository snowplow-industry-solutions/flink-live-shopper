package com.evoura.snowplow.operator.session;

import com.evoura.snowplow.operator.redis.MetricValue;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.util.Collector;

public class SessionFeatureMetricFlatMap implements FlatMapFunction<SessionFeature, MetricValue> {
  private final int ttlSeconds;

  public SessionFeatureMetricFlatMap(int ttlSeconds) {
    this.ttlSeconds = ttlSeconds;
  }

  @Override
  public void flatMap(SessionFeature sessionFeature, Collector<MetricValue> out) throws Exception {
    out.collect(metricOf(sessionFeature, "session_duration", sessionFeature.sessionDuration));

    out.collect(metricOf(sessionFeature, "session_page_count", sessionFeature.pageViewCount));

    out.collect(metricOf(sessionFeature, "session_bounce", hasBounce(sessionFeature)));

    out.collect(
        metricOf(sessionFeature, "session_marketing_campaign", sessionFeature.marketingCampaign));

    out.collect(metricOf(sessionFeature, "session_cart_ratio", getCartRatio(sessionFeature)));

    out.collect(metricOf(sessionFeature, "session_search_count", sessionFeature.searchCount));

    out.collect(
        metricOf(sessionFeature, "shopper_device_types", sessionFeature.deviceTypes.toString()));

    out.collect(metricOf(sessionFeature, "shopper_latest_platform", sessionFeature.latestPlatform));
  }

  private long getCartRatio(SessionFeature sessionFeature) {
    if (sessionFeature.productViews == 0) {
      return 0;
    }

    return sessionFeature.cartAddCount / sessionFeature.productViews;
  }

  private String hasBounce(SessionFeature sessionFeature) {
    return String.valueOf(sessionFeature.pageViewCount < 2);
  }

  private MetricValue metricOf(SessionFeature sessionFeature, String name, long value) {
    return metricOf(sessionFeature, name, String.valueOf(value));
  }

  private MetricValue metricOf(SessionFeature sessionFeature, String name, String value) {
    return new MetricValue(
        createKey(sessionFeature, name), String.valueOf(value), (long) ttlSeconds);
  }

  private String createKey(SessionFeature sessionFeature, String name) {
    return String.format("session:%s:%s", sessionFeature.sessionId, name);
  }
}
