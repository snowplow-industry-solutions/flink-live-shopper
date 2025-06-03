package com.evoura.snowplow.operator.cart;

import static com.evoura.snowplow.operator.StaticContext.MAPPER;

import com.evoura.snowplow.model.SnowplowEvent;
import com.fasterxml.jackson.databind.JsonNode;
import org.apache.flink.api.common.functions.MapFunction;

public class CartEventMap implements MapFunction<SnowplowEvent, CartEvent> {
  public static final String IDENTIFIER = "cart-event-map";

  private static final long serialVersionUID = -3501957969896268783L;

  @Override
  public CartEvent map(SnowplowEvent snowplowEvent) throws Exception {
    JsonNode payloadNode = MAPPER.readTree(snowplowEvent.payload);

    double previousPrice = payloadNode.get("cart").get("total_value").asDouble();
    double productPrice = payloadNode.get("product").get("price").asDouble();

    return new CartEvent(
        snowplowEvent.eventId,
        snowplowEvent.userId,
        snowplowEvent.type,
        previousPrice,
        productPrice,
        snowplowEvent.timestamp);
  }
}
