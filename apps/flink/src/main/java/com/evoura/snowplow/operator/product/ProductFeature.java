package com.evoura.snowplow.operator.product;

import java.io.Serializable;
import java.lang.reflect.Type;
import java.util.HashMap;
import java.util.Map;
import org.apache.flink.api.common.typeinfo.TypeInfoFactory;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeinfo.Types;

/** Result class for average product price calculations */
public class ProductFeature implements Serializable {
  public String userId;
  public int uniqueProductCount;
  public double averagePrice;
  public double minPrice;
  public double maxPrice;
  public String windowSize;
  public Map<String, Long> views;

  public ProductFeature() {}

  public ProductFeature(
      String userId,
      int uniqueProductCount,
      double averagePrice,
      double minPrice,
      double maxPrice,
      String windowSize,
      Map<String, Long> views) {
    this.userId = userId;
    this.uniqueProductCount = uniqueProductCount;
    this.averagePrice = averagePrice;
    this.minPrice = minPrice;
    this.maxPrice = maxPrice;
    this.windowSize = windowSize;
    this.views = views;
  }

  @Override
  public String toString() {
    return String.format(
        "Window: %s, User: %s, Unique Products: %d, Avg Price: %.2f, Min Price: %.2f, Max Price: %.2f, Views: %s",
        windowSize, userId, uniqueProductCount, averagePrice, minPrice, maxPrice, views);
  }

  public static class InfoFactory extends TypeInfoFactory<ProductFeature> {

    public static TypeInformation<ProductFeature> typeInfo() {
      InfoFactory factory = new InfoFactory();
      return factory.createTypeInfo(null, null);
    }

    @Override
    public TypeInformation<ProductFeature> createTypeInfo(
        Type t, Map<String, TypeInformation<?>> genericParameters) {
      Map<String, TypeInformation<?>> fields = new HashMap<>();

      fields.put("userId", Types.STRING);
      fields.put("uniqueProductCount", Types.INT);
      fields.put("averagePrice", Types.DOUBLE);
      fields.put("minPrice", Types.DOUBLE);
      fields.put("maxPrice", Types.DOUBLE);
      fields.put("windowSize", Types.STRING);
      fields.put("views", Types.MAP(Types.STRING, Types.LONG));

      return Types.POJO(ProductFeature.class, fields);
    }
  }
}
