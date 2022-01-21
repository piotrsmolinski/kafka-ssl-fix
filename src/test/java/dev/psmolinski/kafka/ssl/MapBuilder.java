package dev.psmolinski.kafka.ssl;

import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.Map;

/**
 * Utility class to build maps using fluent API.
 * @param <K>
 * @param <V>
 */
public  class MapBuilder<K,V> {

  private Map<K,V> map = new LinkedHashMap<>();

  private MapBuilder(Map<K,V> map) {
    this.map.putAll(map);
  }

  public static <K,V> MapBuilder<K,V> empty(Class<K> keyClass, Class<V> valueClass) {
    return from(Collections.emptyMap());
  }

  public static <K,V> MapBuilder<K,V> empty() {
    return from(Collections.emptyMap());
  }

  public static <K,V> MapBuilder<K,V> from(Map<K,V> map) {
    return new MapBuilder<>(map);
  }

  public MapBuilder<K,V> with(K key, V value) {
    map.put(key, value);
    return this;
  }

  public Map<K,V> build() {
    return map;
  }

}
