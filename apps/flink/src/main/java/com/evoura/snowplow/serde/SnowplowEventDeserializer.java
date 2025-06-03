package com.evoura.snowplow.serde;

import static com.evoura.snowplow.operator.StaticContext.MAPPER;
import static nl.basjes.parse.useragent.AbstractUserAgentAnalyzer.DEFAULT_PARSE_CACHE_SIZE;

import com.evoura.snowplow.model.Enrichment;
import com.evoura.snowplow.model.EventUserAgent;
import com.evoura.snowplow.model.SnowplowEvent;
import com.fasterxml.jackson.databind.JsonNode;
import java.io.IOException;
import java.time.Instant;
import nl.basjes.parse.useragent.UserAgent;
import nl.basjes.parse.useragent.UserAgentAnalyzer;
import org.apache.flink.api.common.serialization.DeserializationSchema;
import org.apache.flink.api.common.typeinfo.TypeInformation;

public class SnowplowEventDeserializer implements DeserializationSchema<SnowplowEvent> {

  private static final long serialVersionUID = 1L;

  private transient UserAgentAnalyzer userAgentAnalyzer;

  @Override
  public void open(InitializationContext context) throws Exception {
    userAgentAnalyzer =
        UserAgentAnalyzer.newBuilder()
            .hideMatcherLoadStats()
            .withField("OperatingSystemClass")
            .withField("OperatingSystemNameVersion")
            .withCache(DEFAULT_PARSE_CACHE_SIZE)
            .build();
  }

  @Override
  public SnowplowEvent deserialize(byte[] message) throws IOException {
    JsonNode rootNode = MAPPER.readTree(message);
    JsonNode enrichNode = rootNode.path("enrich");

    String eventId = rootNode.path("id").asText();
    String eventType = rootNode.path("type").asText();
    String sessionId = rootNode.path("session").path("sessionId").asText();

    String userId = enrichNode.path("user").path("user_id").asText();
    if (userId == null || userId.isEmpty()) {
      userId = enrichNode.path("user").path("domain_userid").asText();
    }

    JsonNode payloadNode = rootNode.path("payload");
    String userAgentStr = enrichNode.path("client").path("useragent").asText();
    UserAgent userAgent = userAgentAnalyzer.parse(userAgentStr);

    Enrichment enrichment = new Enrichment();
    enrichment.platform = enrichNode.path("platform").asText();
    enrichment.user = new Enrichment.User(enrichNode.path("user").path("ipAddress").asText());

    JsonNode marketing = enrichNode.path("marketing");
    if (!marketing.isNull() && !marketing.isMissingNode()) {
      enrichment.marketing =
          new Enrichment.Marketing(
              marketing.path("mkt_campaign").asText(),
              marketing.path("mkt_source").asText(),
              marketing.path("mkt_medium").asText(),
              marketing.path("mkt_term").asText(),
              marketing.path("mkt_content").asText());
    }

    long timestamp = MAPPER.convertValue(rootNode.path("timestamp"), Instant.class).toEpochMilli();

    return new SnowplowEvent(
        eventId,
        eventType,
        sessionId,
        userId,
        payloadNode.toString(),
        new EventUserAgent(
            userAgent.getValue("OperatingSystemClass"),
            userAgent.getValue("OperatingSystemNameVersion")),
        enrichment,
        timestamp);
  }

  @Override
  public boolean isEndOfStream(SnowplowEvent nextElement) {
    return false;
  }

  @Override
  public TypeInformation<SnowplowEvent> getProducedType() {
    return TypeInformation.of(SnowplowEvent.class);
  }
}
