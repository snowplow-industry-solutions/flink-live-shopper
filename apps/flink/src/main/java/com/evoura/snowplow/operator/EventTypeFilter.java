package com.evoura.snowplow.operator;

import com.evoura.snowplow.model.SnowplowEvent;
import java.util.Arrays;
import java.util.List;
import org.apache.flink.api.common.functions.FilterFunction;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class EventTypeFilter implements FilterFunction<SnowplowEvent> {
  public static final String IDENTIFIER = "event-type-filter";

  private static final Logger LOG = LoggerFactory.getLogger(EventTypeFilter.class);
  private static final long serialVersionUID = -5673734482574520206L;

  private final List<String> eventTypes;

  public EventTypeFilter(String... eventTypes) {
    this.eventTypes = Arrays.asList(eventTypes);
  }

  @Override
  public boolean filter(SnowplowEvent event) {
    // Check if the event type is in the list of event types
    boolean isFromType = eventTypes.contains(event.getType());

    if (isFromType) {
      LOG.debug("[{}] Filtering in: {}", eventTypes, event);
      return true;
    }

    return false;
  }
}
