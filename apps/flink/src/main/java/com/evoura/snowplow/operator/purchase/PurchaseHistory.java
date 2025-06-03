package com.evoura.snowplow.operator.purchase;

import java.io.Serializable;
import java.lang.reflect.Type;
import java.util.HashMap;
import java.util.Map;
import org.apache.flink.api.common.typeinfo.TypeInfoFactory;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeinfo.Types;

public class PurchaseHistory implements Serializable {
  public String windowIdentifier;
  public String userId;
  public Map<String, Double> purchases;

  public PurchaseHistory() {}

  public PurchaseHistory(String windowIdentifier, String userId, Map<String, Double> purchases) {
    this.windowIdentifier = windowIdentifier;
    this.userId = userId;
    this.purchases = purchases;
  }

  @Override
  public String toString() {
    return "PurchaseHistory{"
        + "windowIdentifier='"
        + windowIdentifier
        + '\''
        + ", userId='"
        + userId
        + '\''
        + ", purchases="
        + purchases
        + '}';
  }

  public static class InfoFactory extends TypeInfoFactory<PurchaseHistory> {
    public static TypeInformation<PurchaseHistory> typeInfo() {
      InfoFactory factory = new InfoFactory();
      return factory.createTypeInfo(null, null);
    }

    @Override
    public TypeInformation<PurchaseHistory> createTypeInfo(
        Type t, Map<String, TypeInformation<?>> genericParameters) {
      Map<String, TypeInformation<?>> fields = new HashMap<>();

      fields.put("windowIdentifier", Types.STRING);
      fields.put("userId", Types.STRING);
      fields.put("purchases", Types.MAP(Types.STRING, Types.DOUBLE));

      return Types.POJO(PurchaseHistory.class, fields);
    }
  }
}
