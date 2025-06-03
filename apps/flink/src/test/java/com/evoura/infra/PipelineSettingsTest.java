package com.evoura.infra;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import com.evoura.testutils.MockEvent;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import org.apache.flink.api.common.eventtime.Watermark;
import org.apache.flink.api.common.eventtime.WatermarkGenerator;
import org.apache.flink.api.common.eventtime.WatermarkGeneratorSupplier;
import org.apache.flink.api.common.eventtime.WatermarkOutput;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.metrics.MetricGroup;
import org.junit.jupiter.api.Test;

class PipelineSettingsTest {

  @Test
  void testWatermarkGenerationForMockEvents() {
    Duration maxOutOfOrderness = Duration.ofSeconds(5);
    Duration idleTimeout = Duration.ofSeconds(10);

    WatermarkStrategy<MockEvent> strategy =
        PipelineSettings.createWatermarkStrategy(maxOutOfOrderness, idleTimeout);

    List<Watermark> emittedWatermarks = new ArrayList<>();
    WatermarkOutput output =
        new WatermarkOutput() {
          @Override
          public void emitWatermark(Watermark watermark) {
            emittedWatermarks.add(watermark);
          }

          @Override
          public void markIdle() {}

          @Override
          public void markActive() {}
        };

    // Mocking the Context and MetricGroup
    WatermarkGeneratorSupplier.Context context = mock(WatermarkGeneratorSupplier.Context.class);
    when(context.getMetricGroup()).thenReturn(mock(MetricGroup.class));

    WatermarkGenerator<MockEvent> generator = strategy.createWatermarkGenerator(context);

    long baseTimestamp = System.currentTimeMillis();

    generator.onEvent(new MockEvent(baseTimestamp, "e1"), baseTimestamp, output);
    generator.onEvent(new MockEvent(baseTimestamp - 4000, "e2"), baseTimestamp - 4000, output);
    generator.onEvent(new MockEvent(baseTimestamp + 1000, "e3"), baseTimestamp + 1000, output);
    generator.onPeriodicEmit(output);

    assertThat(emittedWatermarks).isNotEmpty();

    long expectedWatermark = (baseTimestamp + 1000) - maxOutOfOrderness.toMillis();
    long actual = emittedWatermarks.get(emittedWatermarks.size() - 1).getTimestamp();

    assertThat(actual)
        .isEqualTo(expectedWatermark)
        .withFailMessage("Expected watermark to be %d, but was %d", expectedWatermark, actual);
  }
}
