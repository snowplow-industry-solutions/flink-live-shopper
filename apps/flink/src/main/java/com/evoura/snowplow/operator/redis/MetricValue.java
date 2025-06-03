package com.evoura.snowplow.operator.redis;

import java.lang.reflect.Type;
import java.util.HashMap;
import java.util.Map;
import org.apache.flink.api.common.typeinfo.TypeInfoFactory;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeinfo.Types;

public class MetricValue {
  public String name;
  public String value;
  public Long ttlSeconds; // Optional TTL in seconds

  public MetricValue() {}

  public MetricValue(String name, String value) {
    this.name = name;
    this.value = value;
    this.ttlSeconds = null;
  }

  public MetricValue(String name, String value, Long ttlSeconds) {
    this.name = name;
    this.value = value;
    this.ttlSeconds = ttlSeconds;
  }

  public static class InfoFactory extends TypeInfoFactory<MetricValue> {

    public static TypeInformation<MetricValue> typeInfo() {
      MetricValue.InfoFactory factory = new MetricValue.InfoFactory();
      return factory.createTypeInfo(null, null);
    }

    @Override
    public TypeInformation<MetricValue> createTypeInfo(
        Type t, Map<String, TypeInformation<?>> genericParameters) {
      Map<String, TypeInformation<?>> fields = new HashMap<>();

      fields.put("name", Types.STRING);
      fields.put("value", Types.STRING);
      fields.put("ttlSeconds", Types.LONG);

      return Types.POJO(MetricValue.class, fields);
    }
  }
}
