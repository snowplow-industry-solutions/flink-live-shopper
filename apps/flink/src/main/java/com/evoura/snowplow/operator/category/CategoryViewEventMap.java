package com.evoura.snowplow.operator.category;

import static com.evoura.snowplow.operator.StaticContext.MAPPER;

import com.evoura.snowplow.model.SnowplowEvent;
import com.fasterxml.jackson.databind.JsonNode;
import org.apache.flink.api.common.functions.MapFunction;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class CategoryViewEventMap implements MapFunction<SnowplowEvent, CategoryViewEvent> {
  public static final String IDENTIFIER = "category-view-event-map";

  private static final Logger LOG = LoggerFactory.getLogger(CategoryViewEventMap.class);
  private static final long serialVersionUID = -1644046419770993091L;

  private static CategoryViewEvent createCategoryView(
      SnowplowEvent snowplowEvent, String category) {
    return new CategoryViewEvent(
        snowplowEvent.getEventId(),
        snowplowEvent.getUserId(),
        category,
        snowplowEvent.getTimestamp());
  }

  @Override
  public CategoryViewEvent map(SnowplowEvent snowplowEvent) throws Exception {
    JsonNode payloadNode = MAPPER.readTree(snowplowEvent.payload);

    JsonNode listName = payloadNode.path("list_name");
    if (!listName.isMissingNode()) {
      return createCategoryView(snowplowEvent, listName.asText(""));
    }

    JsonNode productNode = payloadNode.path("product");
    if (!productNode.isMissingNode()) {
      return createCategoryView(snowplowEvent, productNode.path("category").asText(""));
    }

    LOG.warn(
        "[{}] Missing list_name or product in payload: {}",
        snowplowEvent.getType(),
        snowplowEvent.payload);
    return createCategoryView(snowplowEvent, null);
  }
}
