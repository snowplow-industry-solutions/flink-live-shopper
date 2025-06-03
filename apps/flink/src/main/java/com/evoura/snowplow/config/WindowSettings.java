package com.evoura.snowplow.config;

import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import org.apache.flink.configuration.ReadableConfig;

public class WindowSettings {
  public static final String PRODUCT = "product";
  public static final String CATEGORY = "category";
  public static final String CART = "cart";
  public static final String PURCHASE = "purchase";

  private static final List<String> metricTypes = List.of(PRODUCT, CATEGORY, CART, PURCHASE);
  private static final org.slf4j.Logger LOG =
      org.slf4j.LoggerFactory.getLogger(WindowSettings.class);

  /*
  {
      product:
        5m:
          duration:
          emit:
          ttl:
      session:
        duration:
        emit:
        ttl:
  }
  */
  private final Map<String, Map<String, Config>> rollingConfig;
  private final Config sessionConfig;

  public WindowSettings(Map<String, Map<String, Config>> rollingConfig, Config sessionConfig) {
    this.rollingConfig = rollingConfig;
    this.sessionConfig = sessionConfig;
  }

  public Map<String, Config> rollingConfig(String metricType) {
    return rollingConfig.get(metricType);
  }

  public Config sessionConfig() {
    return sessionConfig;
  }

  public static WindowSettings fromProperties(ReadableConfig configuration) {
    Map<String, String> config = configuration.toMap();

    String sizesConfig = config.get("metric.sizes");
    if (sizesConfig == null) {
      throw new RuntimeException("The config `metric.sizes` is not set");
    }

    List<String> sizes = Arrays.stream(sizesConfig.split(",")).collect(Collectors.toList());

    Map<String, Map<String, Config>> rollingConfig = new HashMap<>();

    metricTypes.forEach(
        metricType -> {
          rollingConfig.put(metricType, new HashMap<>());
          sizes.forEach(
              windowSize -> {
                if (!config.containsKey(
                    getConfigName(windowSize, metricType, "duration_seconds"))) {
                  LOG.debug(
                      "Skipping metric type {} for window size {} as it is not configured",
                      metricType,
                      windowSize);
                  return;
                }

                Map<String, Config> configMap = rollingConfig.get(metricType);

                configMap.put(
                    windowSize,
                    new Config(
                        Integer.parseInt(
                            config.get(getConfigName(windowSize, metricType, "duration_seconds"))),
                        Integer.parseInt(
                            config.get(getConfigName(windowSize, metricType, "emit_seconds"))),
                        Integer.parseInt(
                            config.get(getConfigName(windowSize, metricType, "ttl_seconds")))));

                rollingConfig.put(metricType, configMap);
              });
        });

    Config sessionConfig =
        new Config(
            Integer.parseInt(config.get("metric.session.gap_seconds")),
            Integer.parseInt(config.get("metric.session.emit_seconds")),
            Integer.parseInt(config.get("metric.session.ttl_seconds")));

    return new WindowSettings(rollingConfig, sessionConfig);
  }

  private static String getConfigName(String windowSize, String metricType, String metric) {
    return String.format("metric.%s.%s.%s", metricType, windowSize, metric);
  }

  public static final class Config {
    public final int durationSeconds;
    public final int emitSeconds;
    public final int ttlSeconds;

    public Config(int durationSeconds, int emitSeconds, int ttlSeconds) {
      this.durationSeconds = durationSeconds;
      this.emitSeconds = emitSeconds;
      this.ttlSeconds = ttlSeconds;
    }
  }

  @Override
  public String toString() {
    return "WindowSettings{"
        + "rollingConfig="
        + rollingConfig
        + ", sessionConfig="
        + sessionConfig
        + '}';
  }
}
