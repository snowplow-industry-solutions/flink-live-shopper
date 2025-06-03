package com.evoura.infra;

import com.evoura.operator.TimestampedEvent;
import java.io.IOException;
import java.time.Duration;
import org.apache.flink.api.common.eventtime.Watermark;
import org.apache.flink.api.common.eventtime.WatermarkGenerator;
import org.apache.flink.api.common.eventtime.WatermarkGeneratorSupplier;
import org.apache.flink.api.common.eventtime.WatermarkOutput;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.serialization.DeserializationSchema;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.PipelineOptions;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.ParameterTool;
import org.apache.kafka.clients.consumer.OffsetResetStrategy;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class PipelineSettings {

  private static final Logger LOG = LoggerFactory.getLogger(PipelineSettings.class);

  public static StreamExecutionEnvironment createStreamExecutionEnvironment(
      ParameterTool argsParameters) throws IOException {
    StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
    Configuration configuration = new Configuration();

    String pipelineProps = argsParameters.get("propertiesFile", ".config/flink-local.properties");
    ParameterTool pipelineConfig = ParameterTool.fromPropertiesFile(pipelineProps);

    String windowProps = argsParameters.get("windowPropertiesFile", ".config/window.properties");
    ParameterTool windowConfig = ParameterTool.fromPropertiesFile(windowProps);

    ParameterTool props = pipelineConfig.mergeWith(windowConfig);

    configuration.set(PipelineOptions.GENERIC_TYPES, false);
    configuration.set(PipelineOptions.AUTO_GENERATE_UIDS, false);
    configuration.set(PipelineOptions.AUTO_WATERMARK_INTERVAL, Duration.ofMillis(1_000));
    configuration.addAll(props.getConfiguration());

    env.configure(configuration);

    env.enableCheckpointing(5000);

    if (props.has("flink.parallelism")) {
      env.setParallelism(props.getInt("flink.parallelism"));
    }

    return env;
  }

  public static <T extends TimestampedEvent> WatermarkStrategy<T> createWatermarkStrategy(
      Duration maxOutOfOrderness, Duration idleTimeout) {

    return WatermarkStrategy.<T>forGenerator(
            createWatermarkGeneratorSupplier(maxOutOfOrderness, idleTimeout))
        .withTimestampAssigner((event, timestamp) -> event.getTimestamp());
  }

  public static <T extends TimestampedEvent>
      WatermarkGeneratorSupplier<T> createWatermarkGeneratorSupplier(
          Duration maxOutOfOrderness, Duration idleTimeout) {
    return context -> new PeriodicWatermarkGenerator<>(maxOutOfOrderness, idleTimeout);
  }

  public static <T extends TimestampedEvent> SingleOutputStreamOperator<T> createStream(
      StreamExecutionEnvironment env,
      DeserializationSchema<T> deserializationSchema,
      WatermarkStrategy<T> watermarkStrategy) {

    KafkaSource<T> source =
        KafkaSource.<T>builder()
            .setBootstrapServers(env.getConfiguration().toMap().get("kafka.bootstrap.servers"))
            .setTopics("snowplow-enriched-good")
            // .setGroupId("ecommerce-product-analysis-" + Math.random())
            .setGroupId("ecommerce-product-analysis")
            .setStartingOffsets(OffsetsInitializer.committedOffsets(OffsetResetStrategy.EARLIEST))
            .setValueOnlyDeserializer(deserializationSchema)
            .build();

    return env.fromSource(source, watermarkStrategy, "Kafka Source")
        .uid("KafkaSource")
        .name("KafkaSource");
  }

  public static class PeriodicWatermarkGenerator<T extends TimestampedEvent>
      implements WatermarkGenerator<T> {

    private final long maxOutOfOrderness;
    private final long idleTimeout;

    private long currentMaxTimestamp = 0L;
    private long lastEventProcessingTime = 0L;

    public PeriodicWatermarkGenerator(Duration maxOutOfOrderness, Duration idleTimeout) {
      this.maxOutOfOrderness = maxOutOfOrderness.toMillis();
      this.idleTimeout = idleTimeout.toMillis();
    }

    @Override
    public void onEvent(T event, long eventTimestamp, WatermarkOutput output) {
      lastEventProcessingTime = System.currentTimeMillis();
      currentMaxTimestamp = Math.max(currentMaxTimestamp, event.getTimestamp());
      emitWatermark(output, currentMaxTimestamp);
    }

    @Override
    public void onPeriodicEmit(WatermarkOutput output) {
      long currentProcessingTime = System.currentTimeMillis();

      if (currentProcessingTime - lastEventProcessingTime > idleTimeout) {
        LOG.debug("PERIODIC: Idle timeout reached; emitting watermark based on processing time");
        emitWatermark(output, currentProcessingTime);

        return;
      }

      LOG.debug("PERIODIC: No idle timeout; skipping watermark emission");
    }

    private void emitWatermark(WatermarkOutput output, long timestamp) {
      output.emitWatermark(new Watermark(timestamp - maxOutOfOrderness));
    }
  }
}
