package com.evoura.snowplow.operator.category;

import java.io.Serializable;
import java.lang.reflect.Type;
import java.util.HashMap;
import java.util.Map;
import org.apache.flink.api.common.typeinfo.TypeInfoFactory;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeinfo.Types;

public class CategoryBehavior implements Serializable {
  public String windowIdentifier;
  public String userId;
  public int uniqueCategoryCount;
  public String mostViewedCategory;
  public Map<String, Long> views;

  public CategoryBehavior() {}

  public CategoryBehavior(
      String windowIdentifier,
      String userId,
      int uniqueCategoryCount,
      String mostViewedCategory,
      Map<String, Long> views) {
    this.windowIdentifier = windowIdentifier;
    this.userId = userId;
    this.uniqueCategoryCount = uniqueCategoryCount;
    this.mostViewedCategory = mostViewedCategory;
    this.views = views;
  }

  @Override
  public String toString() {
    return "CategoryBehavior{"
        + "windowIdentifier='"
        + windowIdentifier
        + '\''
        + ", userId='"
        + userId
        + '\''
        + ", uniqueCategoryCount="
        + uniqueCategoryCount
        + ", mostViewedCategory='"
        + mostViewedCategory
        + '\''
        + ", views="
        + views
        + '}';
  }

  public static class InfoFactory extends TypeInfoFactory<CategoryBehavior> {

    public static TypeInformation<CategoryBehavior> typeInfo() {
      InfoFactory factory = new InfoFactory();
      return factory.createTypeInfo(null, null);
    }

    @Override
    public TypeInformation<CategoryBehavior> createTypeInfo(
        Type t, Map<String, TypeInformation<?>> genericParameters) {
      Map<String, TypeInformation<?>> fields = new HashMap<>();

      fields.put("windowIdentifier", Types.STRING);

      fields.put("userId", Types.STRING);
      fields.put("uniqueCategoryCount", Types.INT);
      fields.put("mostViewedCategory", Types.STRING);

      fields.put("views", Types.MAP(Types.STRING, Types.LONG));

      return Types.POJO(CategoryBehavior.class, fields);
    }
  }
}
