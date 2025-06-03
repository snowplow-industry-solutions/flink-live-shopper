package com.evoura.snowplow.model;

import java.io.Serializable;
import java.lang.reflect.Type;
import java.util.HashMap;
import java.util.Map;
import org.apache.flink.api.common.typeinfo.TypeInfoFactory;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeinfo.Types;

public class EventUserAgent implements Serializable {
  public String platform;
  public String type;

  public EventUserAgent() {}

  public EventUserAgent(String platform, String type) {
    this.platform = platform;
    this.type = type;
  }

  public String toString() {
    return "EventUserAgent{" + "platform='" + platform + '\'' + ", type='" + type + '\'' + '}';
  }

  public static class InfoFactory extends TypeInfoFactory<EventUserAgent> {

    public static TypeInformation<EventUserAgent> typeInfo() {
      EventUserAgent.InfoFactory factory = new EventUserAgent.InfoFactory();
      return factory.createTypeInfo(null, null);
    }

    @Override
    public TypeInformation<EventUserAgent> createTypeInfo(
        Type t, Map<String, TypeInformation<?>> genericParameters) {
      Map<String, TypeInformation<?>> fields = new HashMap<>();

      fields.put("platform", Types.STRING);
      fields.put("type", Types.STRING);

      return Types.POJO(EventUserAgent.class, fields);
    }
  }
}
