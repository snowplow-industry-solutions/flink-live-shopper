package com.evoura.snowplow.operator.redis;

import io.lettuce.core.ClientOptions;
import io.lettuce.core.RedisClient;
import io.lettuce.core.RedisFuture;
import io.lettuce.core.RedisURI;
import io.lettuce.core.api.async.RedisAsyncCommands;
import io.lettuce.core.resource.ClientResources;
import io.lettuce.core.resource.DefaultClientResources;
import io.lettuce.core.resource.Delay;
import java.time.Duration;
import java.util.Collections;
import org.apache.flink.api.common.functions.OpenContext;
import org.apache.flink.streaming.api.functions.async.ResultFuture;
import org.apache.flink.streaming.api.functions.async.RichAsyncFunction;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class RedisSink extends RichAsyncFunction<MetricValue, String> {
  private static final Logger LOG = LoggerFactory.getLogger(RedisSink.class);
  private static final int MAX_RETRY_ATTEMPTS = 3;
  private static final long RETRY_DELAY_MS = 1000;

  private final String redisUrl;
  private final Long defaultTTLSeconds;
  private transient RedisClient client = null;
  private transient RedisAsyncCommands<String, String> asyncCall = null;
  private transient ClientResources clientResources = null;

  public RedisSink(String redisUrl, Long defaultTTLSeconds) {
    this.redisUrl = redisUrl;
    this.defaultTTLSeconds = defaultTTLSeconds;
  }

  @Override
  public void open(OpenContext openContext) throws Exception {
    super.open(openContext);

    clientResources = DefaultClientResources.builder().reconnectDelay(Delay.exponential()).build();

    RedisURI redisURI = RedisURI.create(redisUrl);
    redisURI.setTimeout(Duration.ofSeconds(5));

    client = RedisClient.create(clientResources, redisURI);
    client.setOptions(
        ClientOptions.builder()
            .disconnectedBehavior(ClientOptions.DisconnectedBehavior.REJECT_COMMANDS)
            .autoReconnect(true)
            .build());

    boolean connected = connectToRedis();
    if (!connected) {
      throw new RuntimeException("Failed to establish connection to Redis");
    }
  }

  @Override
  public void asyncInvoke(MetricValue metric, ResultFuture<String> resultFuture) {
    retryableAsyncCall(metric, resultFuture, 0);
  }

  @Override
  public void close() {
    closeAsyncConnection();

    if (client != null) {
      client.shutdown();
    }

    if (clientResources != null) {
      clientResources.shutdown();
    }
  }

  private void retryableAsyncCall(
      MetricValue metric, ResultFuture<String> resultFuture, int attemptCount) {
    boolean connected = connectToRedis();
    if (!connected) {
      handleExceptionFlow(
          metric, resultFuture, attemptCount, new RuntimeException("Could not connect to Redis"));
      return;
    }

    RedisFuture<String> setResult = asyncCall.setex(metric.name, getTtl(metric), metric.value);

    setResult.whenComplete(
        (result, throwable) -> {
          if (throwable == null) {
            resultFuture.complete(Collections.singleton(result));
            return;
          }

          handleExceptionFlow(metric, resultFuture, attemptCount, throwable);
        });
  }

  private Long getTtl(MetricValue metric) {
    if (metric == null || metric.ttlSeconds == null) {
      return defaultTTLSeconds;
    }

    return metric.ttlSeconds;
  }

  private void handleExceptionFlow(
      MetricValue metric,
      ResultFuture<String> resultFuture,
      int attemptCount,
      Throwable throwable) {
    LOG.error("Redis operation failed: {}", throwable.getMessage());

    if (attemptCount >= MAX_RETRY_ATTEMPTS) {
      LOG.error("Max retry attempts reached. Failing operation.");
      resultFuture.completeExceptionally(throwable);
      return;
    }

    closeAsyncConnection();

    int nextAttemptCount = attemptCount + 1;
    LOG.info("Retrying Redis operation (attempt {}/{})", nextAttemptCount, MAX_RETRY_ATTEMPTS);
    try {
      Thread.sleep(RETRY_DELAY_MS);

      retryableAsyncCall(metric, resultFuture, nextAttemptCount);
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
      resultFuture.completeExceptionally(throwable);
    }
  }

  private boolean connectToRedis() {
    if (asyncCall != null) {
      return true;
    }

    try {
      asyncCall = client.connect().async();
      LOG.info("Connected to Redis at {}", redisUrl);

      return true;
    } catch (Exception e) {
      LOG.error("Failed to connect to Redis: {}", e.getMessage(), e);
    }

    return false;
  }

  private void closeAsyncConnection() {
    if (asyncCall != null) {
      try {
        asyncCall.getStatefulConnection().close();
      } catch (Exception e) {
        LOG.warn("Error closing Redis connection", e);
      }
      asyncCall = null;
    }
  }
}
