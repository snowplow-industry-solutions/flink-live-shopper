package com.evoura.snowplow.operator.product;

import static com.evoura.snowplow.operator.StaticContext.MAPPER;

import com.evoura.snowplow.model.SnowplowEvent;
import com.fasterxml.jackson.databind.JsonNode;
import org.apache.flink.api.common.functions.MapFunction;

public class ProductViewEventMap implements MapFunction<SnowplowEvent, ProductViewEvent> {
  public static final String IDENTIFIER = "product-view-event-map";

  private static final long serialVersionUID = -8584345139374982669L;

  @Override
  public ProductViewEvent map(SnowplowEvent snowplowEvent) throws Exception {
    String eventId = snowplowEvent.getEventId();
    String userId = snowplowEvent.getUserId();

    String productId = "";
    double productPrice = 0.0;

    JsonNode payloadNode = MAPPER.readTree(snowplowEvent.payload);
    JsonNode productNode = payloadNode.path("product");

    if (!productNode.isMissingNode()) {
      productId = productNode.path("id").asText("");
      JsonNode priceNode = productNode.path("price");
      productPrice = priceNode.isNumber() ? priceNode.asDouble() : 0.0;
    }

    return new ProductViewEvent(
        eventId, userId, productId, productPrice, snowplowEvent.getTimestamp());
  }
}
