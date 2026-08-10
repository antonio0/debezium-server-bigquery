package io.debezium.server.bigquery;

import com.fasterxml.jackson.databind.node.JsonNodeFactory;
import io.debezium.DebeziumException;
import io.debezium.runtime.BatchEvent;
import io.debezium.runtime.CapturingEvents;
import io.debezium.server.bigquery.batchsizewait.BatchSizeWait;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.lang.reflect.Field;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Semaphore;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;

@Tag("unit")
class BaseChangeConsumerParallelTest {
  private TestConsumer consumer;

  @AfterEach
  void closeExecutor() {
    if (consumer != null) {
      consumer.shutdownExecutors();
    }
  }

  @Test
  void destinationFailurePreventsAllCommits() throws Exception {
    consumer = configuredConsumer(2, 1);
    consumer.failedDestination = "bad";
    List<BatchEvent> records = List.of(event("good"), event("bad"));

    assertThrows(DebeziumException.class, () -> consumer.handle(events(records)));
    records.forEach(record -> verify(record, never()).commit());
  }

  @Test
  void timeoutPreventsOffsetAdvancement() throws Exception {
    consumer = configuredConsumer(2, 0);
    consumer.blockUploads = true;
    List<BatchEvent> records = List.of(event("slow"));

    assertThrows(DebeziumException.class, () -> consumer.handle(events(records)));
    verify(records.get(0), never()).commit();
  }

  @Test
  void successfulDestinationsCommitEveryRecord() throws Exception {
    consumer = configuredConsumer(2, 1);
    List<BatchEvent> records = List.of(event("one"), event("two"));

    consumer.handle(events(records));
    records.forEach(record -> verify(record, times(1)).commit());
  }

  @Test
  void interruptedSemaphoreAcquisitionDoesNotReleaseUnacquiredPermit() throws Exception {
    consumer = configuredConsumer(2, 1);
    ObservedSemaphore semaphore = new ObservedSemaphore();
    setField(consumer, "concurrencyLimiter", semaphore);
    AtomicReference<Throwable> thrown = new AtomicReference<>();
    AtomicBoolean interruptPreserved = new AtomicBoolean();
    Thread caller = new Thread(() -> {
      try {
        consumer.processTablesInParallel(Map.of("blocked", List.of(event("blocked"))));
      } catch (Throwable e) {
        thrown.set(e);
        interruptPreserved.set(Thread.currentThread().isInterrupted());
      }
    });
    caller.start();
    semaphore.acquisitionAttempted.await();
    caller.interrupt();
    caller.join(5000);

    assertFalse(caller.isAlive());
    assertNotNull(thrown.get());
    assertTrue(thrown.get() instanceof DebeziumException);
    assertTrue(interruptPreserved.get());
    assertEquals(0, semaphore.availablePermits());
  }

  private static TestConsumer configuredConsumer(int concurrency, int timeoutMinutes) throws Exception {
    TestConsumer result = new TestConsumer();
    result.debeziumConfig = mock(DebeziumConfig.class);
    when(result.debeziumConfig.topicHeartbeatPrefix()).thenReturn("__heartbeat");
    when(result.debeziumConfig.topicHeartbeatSkipConsuming()).thenReturn(false);
    result.batchSizeWait = mock(BatchSizeWait.class);
    setField(result, "numConcurrentUploads", concurrency);
    setField(result, "concurrentUploadsTimeoutMinutes", timeoutMinutes);
    setField(result, "concurrencyLimiter", new Semaphore(concurrency));
    return result;
  }

  private static void setField(Object target, String name, Object value) throws Exception {
    Field field = BaseChangeConsumer.class.getDeclaredField(name);
    field.setAccessible(true);
    field.set(target, value);
  }

  private static BatchEvent event(String destination) {
    BatchEvent event = mock(BatchEvent.class);
    when(event.destination()).thenReturn(destination);
    return event;
  }

  @SuppressWarnings("unchecked")
  private static CapturingEvents<BatchEvent> events(List<BatchEvent> records) {
    CapturingEvents<BatchEvent> events = mock(CapturingEvents.class);
    when(events.records()).thenReturn(records);
    return events;
  }

  private static class TestConsumer extends BaseChangeConsumer {
    private String failedDestination;
    private boolean blockUploads;
    private final CountDownLatch blocker = new CountDownLatch(1);

    @Override
    public long uploadDestination(String destination, List<RecordConverter> data) {
      if (destination.equals(failedDestination)) {
        throw new DebeziumException("deliberate failure");
      }
      if (blockUploads) {
        try {
          blocker.await();
        } catch (InterruptedException e) {
          Thread.currentThread().interrupt();
          throw new DebeziumException("cancelled upload", e);
        }
      }
      return data.size();
    }

    @Override
    public RecordConverter eventAsRecordConverter(BatchEvent event) {
      RecordConverter converter = mock(RecordConverter.class);
      when(converter.valueSchema()).thenReturn(JsonNodeFactory.instance.objectNode());
      return converter;
    }
  }

  private static class ObservedSemaphore extends Semaphore {
    private final CountDownLatch acquisitionAttempted = new CountDownLatch(1);

    private ObservedSemaphore() {
      super(0);
    }

    @Override
    public void acquire() throws InterruptedException {
      acquisitionAttempted.countDown();
      super.acquire();
    }
  }
}
