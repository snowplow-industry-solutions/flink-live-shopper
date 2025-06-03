package com.evoura.snowplow.operator.purchase;

import com.evoura.operator.RollingWindowProcessFunction;
import com.evoura.snowplow.model.SnowplowEvent;
import com.evoura.snowplow.operator.StaticContext;
import com.fasterxml.jackson.databind.JsonNode;
import java.time.Duration;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import java.util.stream.StreamSupport;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.typeutils.ListTypeInfo;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;

public class PurchaseHistoryRollingWindow
    extends RollingWindowProcessFunction<String, SnowplowEvent, PurchaseHistory> {
  public static final String IDENTIFIER = "purchase-history-rolling-window";

  private static final long serialVersionUID = 2098818366603629736L;

  private static final TypeInformation<SnowplowEvent> TYPE_INFO =
      ListTypeInfo.of(SnowplowEvent.class);

  private final String windowIdentifier;

  public PurchaseHistoryRollingWindow(
      Duration windowSize, Duration emitInterval, String windowIdentifier) {
    super(windowSize, emitInterval, TYPE_INFO);

    this.windowIdentifier = windowIdentifier;
  }

  @Override
  public void processWindow(
      Iterable<SnowplowEvent> events,
      KeyedProcessFunction<String, SnowplowEvent, PurchaseHistory>.OnTimerContext ctx,
      Collector<PurchaseHistory> out)
      throws Exception {
    Map<String, Double> purchases = new HashMap<>();
    List<SnowplowEvent> eventList =
        StreamSupport.stream(events.spliterator(), false)
            .sorted(Comparator.comparing(SnowplowEvent::getTimestamp))
            .collect(Collectors.toList());

    SnowplowEvent lastCartEvent = null;

    for (SnowplowEvent event : eventList) {
      String type = event.getType();

      if ("add_to_cart".equals(type) || "remove_from_cart".equals(type)) {
        lastCartEvent = event;

      } else if ("checkout_step".equals(type)) {
        JsonNode payload;
        payload = StaticContext.MAPPER.readTree(event.payload);

        if (payload.has("checkout_step") && payload.get("checkout_step").get("step").asInt() == 2) {
          if (lastCartEvent != null) {
            JsonNode cartPayload = StaticContext.MAPPER.readTree(lastCartEvent.payload);
            double cartTotal = cartPayload.path("cart").path("total_value").asDouble(0.0);
            double productPrice = cartPayload.path("product").path("price").asDouble(0.0);

            double updatedTotal =
                "add_to_cart".equals(lastCartEvent.getType())
                    ? cartTotal + productPrice
                    : cartTotal - productPrice;

            purchases.put(event.getEventId(), updatedTotal);

            lastCartEvent = null;
          }
        }
      }
    }

    out.collect(new PurchaseHistory(windowIdentifier, ctx.getCurrentKey(), purchases));
  }
}
