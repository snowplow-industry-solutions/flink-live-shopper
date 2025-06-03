package com.evoura.snowplow.operator.session;

import java.io.Serializable;
import java.lang.reflect.Type;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.apache.flink.api.common.typeinfo.TypeInfoFactory;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeinfo.Types;

// TODO: missing location and region
public class SessionFeature implements Serializable {
  private static final long serialVersionUID = 1L;

  public long windowStart;

  public String sessionId;
  public long sessionDuration;

  public String userId;

  public long pageViewCount;
  public long searchCount;

  public long productViews;

  public long cartAddCount;

  // utm_campaign
  public String marketingCampaign;

  public List<String> deviceTypes;
  public String latestPlatform;

  public SessionFeature() {}

  public SessionFeature(
      long windowStart,
      String sessionId,
      long sessionDuration,
      String userId,
      long pageViewCount,
      long searchCount,
      long productViews,
      long cartAddCount,
      String marketingCampaign,
      List<String> deviceTypes,
      String latestPlatform) {
    this.windowStart = windowStart;
    this.sessionId = sessionId;
    this.sessionDuration = sessionDuration;
    this.userId = userId;
    this.pageViewCount = pageViewCount;
    this.searchCount = searchCount;
    this.productViews = productViews;
    this.cartAddCount = cartAddCount;
    this.marketingCampaign = marketingCampaign;
    this.deviceTypes = deviceTypes;
    this.latestPlatform = latestPlatform;
  }

  @Override
  public String toString() {
    return "SessionFeature{"
        + "windowStart="
        + windowStart
        + ", sessionId='"
        + sessionId
        + '\''
        + ", sessionDuration="
        + sessionDuration
        + ", userId='"
        + userId
        + '\''
        + ", pageViewCount="
        + pageViewCount
        + ", searchCount="
        + searchCount
        + ", productViews="
        + productViews
        + ", cartAddCount="
        + cartAddCount
        + ", marketingCampaign='"
        + marketingCampaign
        + '\''
        + ", deviceTypes="
        + deviceTypes
        + ", latestPlatform='"
        + latestPlatform
        + '\''
        + '}';
  }

  public static class InfoFactory extends TypeInfoFactory<SessionFeature> {

    public static TypeInformation<SessionFeature> typeInfo() {
      SessionFeature.InfoFactory factory = new SessionFeature.InfoFactory();
      return factory.createTypeInfo(null, null);
    }

    @Override
    public TypeInformation<SessionFeature> createTypeInfo(
        Type t, Map<String, TypeInformation<?>> genericParameters) {
      Map<String, TypeInformation<?>> fields = new HashMap<>();

      fields.put("windowStart", Types.LONG);

      fields.put("sessionId", Types.STRING);
      fields.put("sessionDuration", Types.LONG);

      fields.put("userId", Types.STRING);

      fields.put("pageViewCount", Types.LONG);
      fields.put("searchCount", Types.LONG);

      fields.put("productViews", Types.LONG);

      fields.put("cartAddCount", Types.LONG);

      fields.put("marketingCampaign", Types.STRING);

      fields.put("deviceTypes", Types.LIST(Types.STRING));
      fields.put("latestPlatform", Types.STRING);

      return Types.POJO(SessionFeature.class, fields);
    }
  }
}
