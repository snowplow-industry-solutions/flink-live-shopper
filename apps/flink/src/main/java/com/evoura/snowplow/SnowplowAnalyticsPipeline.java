package com.evoura.snowplow;

import com.evoura.infra.PipelineSettings;
import com.evoura.snowplow.config.WindowSettings;
import com.evoura.snowplow.model.SnowplowEvent;
import com.evoura.snowplow.operator.EventTypeFilter;
import com.evoura.snowplow.operator.cart.CartBehavior;
import com.evoura.snowplow.operator.cart.CartBehaviorMetricFlatMap;
import com.evoura.snowplow.operator.cart.CartBehaviorRollingWindow;
import com.evoura.snowplow.operator.cart.CartEvent;
import com.evoura.snowplow.operator.cart.CartEventMap;
import com.evoura.snowplow.operator.category.CategoryBehavior;
import com.evoura.snowplow.operator.category.CategoryBehaviorRollingWindow;
import com.evoura.snowplow.operator.category.CategoryMetricsGeneratorRichFunction;
import com.evoura.snowplow.operator.category.CategoryViewEvent;
import com.evoura.snowplow.operator.category.CategoryViewEventMap;
import com.evoura.snowplow.operator.product.ProductFeature;
import com.evoura.snowplow.operator.product.ProductFeatureRollingWindow;
import com.evoura.snowplow.operator.product.ProductMetricsGeneratorRichFunction;
import com.evoura.snowplow.operator.product.ProductViewEvent;
import com.evoura.snowplow.operator.product.ProductViewEventMap;
import com.evoura.snowplow.operator.purchase.PurchaseHistory;
import com.evoura.snowplow.operator.purchase.PurchaseHistoryFlatMap;
import com.evoura.snowplow.operator.purchase.PurchaseHistoryRollingWindow;
import com.evoura.snowplow.operator.redis.MetricValue;
import com.evoura.snowplow.operator.redis.RedisSink;
import com.evoura.snowplow.operator.session.SessionFeature;
import com.evoura.snowplow.operator.session.SessionFeatureMetricFlatMap;
import com.evoura.snowplow.operator.session.SnowplowSessionWindow;
import com.evoura.snowplow.serde.SnowplowEventDeserializer;
import java.time.Duration;
import java.util.concurrent.TimeUnit;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.streaming.api.datastream.AsyncDataStream;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.PrintSink;
import org.apache.flink.util.ParameterTool;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Defines and executes the Flink streaming pipeline for real-time Snowplow event analytics.
 *
 * <p>This pipeline consumes raw Snowplow event data (assumed to be JSON strings) from a Kafka topic
 * configured via {@link PipelineSettings}, parses them into structured {@link SnowplowEvent}
 * objects, assigns watermarks, and then processes them through multiple parallel analytical
 * branches to derive user behavior metrics and features in real-time.
 *
 * <h2>Pipeline Data Flow:</h2>
 *
 * <p>The {@code main} method orchestrates the following steps:
 *
 * <ol>
 *   <li><b>Environment Setup:</b> Creates the {@link StreamExecutionEnvironment} using {@link
 *       PipelineSettings#createStreamExecutionEnvironment}.
 *   <li><b>Source & Parsing:</b> Creates a {@link DataStream} of {@link SnowplowEvent} objects from
 *       Kafka using {@link PipelineSettings#createStream}, employing the {@link
 *       SnowplowEventDeserializer} and a watermark strategy via {@link
 *       PipelineSettings#createWatermarkStrategy}.
 *   <li><b>Analytical Branching:</b> The stream of parsed events is split and fed into multiple
 *       independent processing paths based on event type and desired analysis:
 *       <ul>
 *         <li><b>Product Features:</b> Filters for {@code product_view} events, keys by {@code
 *             userId}, and processes events in multiple {@link
 *             com.evoura.operator.RollingWindowProcessFunction rolling windows} (e.g., 5m, 1h)
 *             using {@link ProductFeatureRollingWindow}. Results are further processed by {@link
 *             ProductMetricsGeneratorRichFunction} to generate {@link MetricValue}s.
 *         <li><b>Category Behavior:</b> Filters for {@code list_view} and {@code product_view}
 *             events, keys by {@code userId}, processes in multiple rolling windows (e.g., 5m, 1h,
 *             24h) via {@link CategoryBehaviorRollingWindow}, and generates metrics using {@link
 *             CategoryMetricsGeneratorRichFunction}.
 *         <li><b>Cart Behavior:</b> Filters for {@code add_to_cart} and {@code remove_from_cart}
 *             events, keys by {@code userId}, processes in rolling windows (e.g., 5m, 1h) with
 *             {@link CartBehaviorRollingWindow}, and generates metrics via {@link
 *             CartBehaviorMetricFlatMap}.
 *         <li><b>Purchase History:</b> Filters for {@code purchase} events, keys by {@code userId},
 *             (potentially using a custom ProcessFunction or window) to aggregate purchase data
 *             (e.g., total spend, item count, frequency) and generates {@link MetricValue}s.
 *         <li><b>Session Features:</b> Filters for various engagement events (page views, product
 *             views, cart actions), keys by {@code sessionId}, and groups events into sessions
 *             using a {@link com.evoura.operator.SessionWindowProcessFunction session window}
 *             implemented by {@link SnowplowSessionWindow}. Session features are then converted to
 *             metrics by {@link SessionFeatureMetricFlatMap}.
 *       </ul>
 *   <li><b>Sink:</b> The {@link MetricValue} streams generated by each analytical branch are
 *       written asynchronously to Redis using {@link RedisSink} via the {@link #addRedisSink}
 *       helper. Debug output is optionally printed using {@link #addPrintSink}.
 *   <li><b>Execution:</b> Starts the Flink job execution via {@code env.execute()}.
 * </ol>
 *
 * <h2>Window Processing Visualization (Conceptual):</h2>
 *
 * <p><b>Rolling Window (e.g., Product Features, Window Size=10min, Emit=1min):</b>
 *
 * <pre>
 * Input Events (keyed by UserID U1):
 * [PV1@T=0] [PV2@T=2] [PV3@T=7] [PV4@T=11] [PV5@T=13]
 * --> Time
 *
 * Watermark -> W@T=0   W@T=1   W@T=2   ... W@T=10  W@T=11  W@T=12  W@T=13
 *
 * Emit @ T=1 (Window=[T-9, T1]): process([PV1]) -> Output1
 * Emit @ T=2 (Window=[T-8, T2]): process([PV1, PV2]) -> Output2
 * ...
 * Emit @ T=11 (Window=[T1, T11]): process([PV2, PV3, PV4]) (PV1 pruned) -> Output11
 * Emit @ T=12 (Window=[T2, T12]): process([PV2, PV3, PV4]) -> Output12
 * Emit @ T=13 (Window=[T3, T13]): process([PV3, PV4, PV5]) (PV2 pruned) -> Output13
 * </pre>
 *
 * <p><b>Session Window (e.g., Session Features, Max Gap=30min):</b>
 *
 * <pre>
 * Input Events (keyed by SessionID S1):
 * [E1@T=0] [E2@T=5]   [E3@T=40] [E4@T=45] [E5@T=80]
 * --> Time
 *
 * Session 1:
 * Events: E1, E2
 * Activity: T=0 to T=5. Gap E2 -> E3 (35min) > Max Gap (30min).
 * Close Time: T = 5 + 30 = 35
 * Process @ T=35: process([E1, E2]) -> OutputS1_1
 *
 * Session 2:
 * Events: E3, E4
 * Activity: T=40 to T=45. Gap E4 -> E5 (35min) > Max Gap (30min).
 * Close Time: T = 45 + 30 = 75
 * Process @ T=75: process([E3, E4]) -> OutputS1_2
 *
 * Session 3:
 * Events: E5
 * Activity: T=80. (Assuming end of stream or next event gap > 30min)
 * Close Time: T = 80 + 30 = 110
 * Process @ T=110: process([E5]) -> OutputS1_3
 * </pre>
 *
 * <p>The pipeline uses standard Flink operators and custom implementations (in {@code
 * com.evoura.operator} and {@code com.evoura.snowplow.operator}) to perform these tasks.
 *
 * @see StreamExecutionEnvironment
 * @see DataStream
 * @see SnowplowEvent
 * @see com.evoura.operator.RollingWindowProcessFunction
 * @see com.evoura.operator.SessionWindowProcessFunction
 * @see MetricValue
 * @see PipelineSettings
 * @see RedisSink
 */
public class SnowplowAnalyticsPipeline {

  private static final Logger LOG = LoggerFactory.getLogger(SnowplowAnalyticsPipeline.class);

  // default TTL is 15 minutes
  private static final long DEFAULT_REDIS_TTL = 15 * 60;

  public static void main(String[] args) throws Exception {
    final ParameterTool argsParameters = ParameterTool.fromArgs(args);

    StreamExecutionEnvironment env =
        PipelineSettings.createStreamExecutionEnvironment(argsParameters);

    WatermarkStrategy<SnowplowEvent> watermarkStrategy =
        PipelineSettings.createWatermarkStrategy(Duration.ofSeconds(5), Duration.ofSeconds(3));

    SingleOutputStreamOperator<SnowplowEvent> parsedEvents =
        PipelineSettings.createStream(env, new SnowplowEventDeserializer(), watermarkStrategy)
            .disableChaining();

    WindowSettings windowSettings = WindowSettings.fromProperties(env.getConfiguration());

    productFeature(parsedEvents, windowSettings);

    categoryBehavior(parsedEvents, windowSettings);

    cartBehavior(parsedEvents, windowSettings);

    purchaseHistory(parsedEvents, windowSettings);

    sessionWindow(parsedEvents, windowSettings);

    env.execute("Ecommerce Product Analysis Pipeline");
  }

  private static void productFeature(
      DataStream<SnowplowEvent> parsedEvents, WindowSettings windowSettings) {
    String filterUid = EventTypeFilter.IDENTIFIER + "-product_view";
    KeyedStream<ProductViewEvent, String> stream =
        parsedEvents
            .filter(new EventTypeFilter("product_view"))
            .uid(filterUid)
            .name(filterUid)
            .map(new ProductViewEventMap())
            .uid(ProductViewEventMap.IDENTIFIER)
            .name(ProductViewEventMap.IDENTIFIER)
            .keyBy(productView -> productView.userId);

    windowSettings
        .rollingConfig(WindowSettings.PRODUCT)
        .forEach(
            (windowIdentifier, config) -> {
              LOG.info(
                  "Creating product view window for {} with config: {}", windowIdentifier, config);

              createProductViewWindow(
                  stream,
                  Duration.ofSeconds(config.durationSeconds),
                  Duration.ofSeconds(config.emitSeconds),
                  windowIdentifier,
                  config.ttlSeconds);
            });
  }

  private static void createProductViewWindow(
      KeyedStream<ProductViewEvent, String> stream,
      Duration windowSize,
      Duration emitInterval,
      String windowIdentifier,
      int ttlSeconds) {
    String processUid = ProductFeatureRollingWindow.IDENTIFIER + "-" + windowIdentifier;
    DataStream<ProductFeature> windowStream =
        stream
            .process(new ProductFeatureRollingWindow(windowSize, emitInterval, windowIdentifier))
            .returns(ProductFeature.InfoFactory.typeInfo())
            .uid(processUid)
            .name(processUid);
    addPrintSink(windowStream, windowIdentifier);

    String metricsUid = "product-metrics-" + windowIdentifier;
    DataStream<MetricValue> productMetrics =
        windowStream
            .process(new ProductMetricsGeneratorRichFunction(ttlSeconds))
            .returns(MetricValue.InfoFactory.typeInfo())
            .uid(metricsUid)
            .name(metricsUid);
    addRedisSink(productMetrics, metricsUid);
  }

  private static void categoryBehavior(
      DataStream<SnowplowEvent> parsedEvents, WindowSettings windowSettings) {
    String filterUid = EventTypeFilter.IDENTIFIER + "-list_view";
    KeyedStream<CategoryViewEvent, String> stream =
        parsedEvents
            .filter(new EventTypeFilter("list_view", "product_view"))
            .uid(filterUid)
            .name(filterUid)
            .map(new CategoryViewEventMap())
            .uid(CategoryViewEventMap.IDENTIFIER)
            .name(CategoryViewEventMap.IDENTIFIER)
            .keyBy(categoryView -> categoryView.userId);

    windowSettings
        .rollingConfig(WindowSettings.CATEGORY)
        .forEach(
            (windowIdentifier, config) -> {
              LOG.info(
                  "Creating category behavior window for {} with config: {}",
                  windowIdentifier,
                  config);

              createCategoryBehaviorWindow(
                  stream,
                  Duration.ofSeconds(config.durationSeconds),
                  Duration.ofSeconds(config.emitSeconds),
                  windowIdentifier,
                  config.ttlSeconds);
            });
  }

  private static void createCategoryBehaviorWindow(
      KeyedStream<CategoryViewEvent, String> stream,
      Duration windowSize,
      Duration emitInterval,
      String windowIdentifier,
      int ttlSeconds) {
    String windowUid = CategoryBehaviorRollingWindow.IDENTIFIER + "-" + windowIdentifier;
    DataStream<CategoryBehavior> windowStream =
        stream
            .process(new CategoryBehaviorRollingWindow(windowSize, emitInterval, windowIdentifier))
            .returns(CategoryBehavior.InfoFactory.typeInfo())
            .uid(windowUid)
            .name(windowUid);
    addPrintSink(windowStream, windowIdentifier);

    String metricsUid = "category-metrics-" + windowIdentifier;
    DataStream<MetricValue> categoryMetrics =
        windowStream
            .process(new CategoryMetricsGeneratorRichFunction(ttlSeconds))
            .returns(MetricValue.InfoFactory.typeInfo())
            .uid(metricsUid)
            .name(metricsUid);
    addRedisSink(categoryMetrics, metricsUid);
  }

  private static void cartBehavior(
      DataStream<SnowplowEvent> parsedEvents, WindowSettings windowSettings) {
    String filterUid = EventTypeFilter.IDENTIFIER + "-cart";
    KeyedStream<CartEvent, String> stream =
        parsedEvents
            .filter(new EventTypeFilter("add_to_cart", "remove_from_cart"))
            .uid(filterUid)
            .name(filterUid)
            .map(new CartEventMap())
            .uid(CartEventMap.IDENTIFIER)
            .name(CartEventMap.IDENTIFIER)
            .keyBy(cartEvent -> cartEvent.userId);

    windowSettings
        .rollingConfig(WindowSettings.CART)
        .forEach(
            (windowIdentifier, config) -> {
              LOG.info(
                  "Creating cart behavior window for {} with config: {}", windowIdentifier, config);

              createCartBehaviorWindow(
                  stream,
                  Duration.ofSeconds(config.durationSeconds),
                  Duration.ofSeconds(config.emitSeconds),
                  windowIdentifier,
                  config.ttlSeconds);
            });
  }

  private static void createCartBehaviorWindow(
      KeyedStream<CartEvent, String> stream,
      Duration windowSize,
      Duration emitInterval,
      String windowIdentifier,
      int ttlSeconds) {
    String processUid = CartBehaviorRollingWindow.IDENTIFIER + "-" + windowIdentifier;
    DataStream<CartBehavior> windowStream =
        stream
            .process(new CartBehaviorRollingWindow(windowSize, emitInterval, windowIdentifier))
            .uid(processUid)
            .name(processUid);
    addPrintSink(windowStream, windowIdentifier);

    String metricUid = CartBehaviorMetricFlatMap.IDENTIFIER + "-" + windowIdentifier;
    DataStream<MetricValue> metricStream =
        windowStream
            .flatMap(new CartBehaviorMetricFlatMap(ttlSeconds))
            .returns(MetricValue.InfoFactory.typeInfo())
            .uid(metricUid)
            .name(metricUid);
    addRedisSink(metricStream, metricUid);
  }

  private static void purchaseHistory(
      DataStream<SnowplowEvent> parsedEvents, WindowSettings windowSettings) {
    String filterUid = EventTypeFilter.IDENTIFIER + "-purchase";
    KeyedStream<SnowplowEvent, String> stream =
        parsedEvents
            .filter(new EventTypeFilter("add_to_cart", "remove_from_cart", "checkout_step"))
            .uid(filterUid)
            .name(filterUid)
            .keyBy(snowplowEvent -> snowplowEvent.userId);

    windowSettings
        .rollingConfig(WindowSettings.PURCHASE)
        .forEach(
            (windowIdentifier, config) -> {
              LOG.info(
                  "Creating purchase history window for {} with config: {}",
                  windowIdentifier,
                  config);

              createPurchaseHistoryWindow(
                  stream,
                  Duration.ofSeconds(config.durationSeconds),
                  Duration.ofSeconds(config.emitSeconds),
                  windowIdentifier,
                  config.ttlSeconds);
            });
  }

  private static void createPurchaseHistoryWindow(
      KeyedStream<SnowplowEvent, String> stream,
      Duration windowSize,
      Duration emitInterval,
      String windowIdentifier,
      int ttlSeconds) {
    String processUid = PurchaseHistoryRollingWindow.IDENTIFIER + "-" + windowIdentifier;
    DataStream<PurchaseHistory> windowStream =
        stream
            .process(new PurchaseHistoryRollingWindow(windowSize, emitInterval, windowIdentifier))
            .uid(processUid)
            .name(processUid)
            .returns(PurchaseHistory.InfoFactory.typeInfo());
    addPrintSink(windowStream, windowIdentifier);

    String metricUid = PurchaseHistoryFlatMap.IDENTIFIER + "-" + windowIdentifier;
    DataStream<MetricValue> metricStream =
        windowStream
            .flatMap(new PurchaseHistoryFlatMap(ttlSeconds))
            .returns(MetricValue.InfoFactory.typeInfo())
            .uid(metricUid)
            .name(metricUid);
    addRedisSink(metricStream, metricUid);
  }

  private static void sessionWindow(
      DataStream<SnowplowEvent> parsedEvents, WindowSettings windowSettings) {
    String filterUid = EventTypeFilter.IDENTIFIER + "-session";
    KeyedStream<SnowplowEvent, String> stream =
        parsedEvents
            .filter(
                new EventTypeFilter("page_view", "product_view", "add_to_cart", "remove_from_cart"))
            .uid(filterUid)
            .name(filterUid)
            .keyBy(SnowplowEvent::getSessionId);

    WindowSettings.Config sessionConfig = windowSettings.sessionConfig();

    String processUid = "session-window";
    DataStream<SessionFeature> windowStream =
        stream
            .process(
                new SnowplowSessionWindow(
                    Duration.ofSeconds(sessionConfig.emitSeconds),
                    Duration.ofSeconds(sessionConfig.durationSeconds)))
            .uid(processUid)
            .name(processUid)
            .returns(SessionFeature.InfoFactory.typeInfo());

    addPrintSink(windowStream, "session");

    String metricUid = "metric-session-window";
    DataStream<MetricValue> metricStream =
        windowStream
            .flatMap(new SessionFeatureMetricFlatMap(sessionConfig.ttlSeconds))
            .uid(metricUid)
            .name(metricUid);
    addRedisSink(metricStream, metricUid);
  }

  private static void addPrintSink(DataStream<?> stream, String identifier) {
    stream
        .sinkTo(new PrintSink<>(identifier, false))
        .uid(identifier + "WindowSink-" + stream.getId())
        .name(identifier + "WindowSink-" + stream.getId());
  }

  private static void addRedisSink(DataStream<MetricValue> stream, String identifier) {
    String redisUrl =
        String.format(
            "redis://%s:%s",
            stream.getExecutionEnvironment().getConfiguration().toMap().get("redis.host"),
            stream.getExecutionEnvironment().getConfiguration().toMap().get("redis.port"));

    AsyncDataStream.orderedWait(
            stream, new RedisSink(redisUrl, DEFAULT_REDIS_TTL), 1000, TimeUnit.MILLISECONDS, 100)
        .uid("metrics-sink-" + identifier);
  }
}
