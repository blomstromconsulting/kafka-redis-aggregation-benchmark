package io.aggbench.generator;

import io.aggbench.domain.FragmentEvent;
import io.aggbench.domain.Json;
import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.MeterRegistry;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.ByteArraySerializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.jboss.logging.Logger;

import java.time.Instant;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.PriorityQueue;
import java.util.Properties;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.LockSupport;

/**
 * Open-loop load generation. Events are sent at their planned times, independent of how fast the system under
 * test consumes them; if the producer itself falls behind (e.g. buffer full) the lateness is measured as
 * "schedule lag" and the requested rate is not claimed as achieved.
 *
 * <p>Lifecycle of one run: plan → manifest (COPY to PostgreSQL) → warm-up (with ramp) → measurement →
 * flush → drain (wait until every lag-monitored group has zero lag) → finish.
 */
final class LoadGenerator {

    private static final Logger LOG = Logger.getLogger(LoadGenerator.class);

    private record Scheduled(long dueNanos, long seq, WorkloadPlan.AggregatePlan plan, WorkloadPlan.PlannedEvent event) {
    }

    private final GeneratorConfig config;
    private final MeterRegistry registry;
    private final AtomicLong sent = new AtomicLong();
    private final AtomicLong acked = new AtomicLong();
    private final AtomicLong failed = new AtomicLong();
    private final AtomicLong intervalMaxScheduleLagMs = new AtomicLong();
    private final AtomicLong maxScheduleLagMs = new AtomicLong();

    LoadGenerator(GeneratorConfig config, MeterRegistry registry) {
        this.config = config;
        this.registry = registry;
    }

    static long nowMicros() {
        Instant now = Instant.now();
        return now.getEpochSecond() * 1_000_000L + now.getNano() / 1_000L;
    }

    int run() throws Exception {
        WorkloadPlan plan = new WorkloadPlan(config.planSettings());
        List<LagMonitor.Target> targets = parseTargets(config.lagGroups());
        try (RunRecorder recorder = new RunRecorder(config.jdbcUrl(), config.jdbcUser(), config.jdbcPassword(), config.runId());
             LagMonitor lagMonitor = new LagMonitor(config.bootstrapServers(), targets, registry);
             KafkaProducer<String, byte[]> producer = new KafkaProducer<>(producerProperties())) {

            recorder.insertRun(config.variant(), config.scenario(), config.repetition(), configSnapshot(),
                    nowMicros(), plan.totalAggregates(), 0, config.eventRate());
            long manifestStart = System.nanoTime();
            long plannedEvents = recorder.writeManifest(plan);
            LOG.infof("Manifest written: %d aggregates, %d events in %d ms", plan.totalAggregates(), plannedEvents,
                    TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - manifestStart));
            producer.partitionsFor(config.ingressTopic());

            ScheduledExecutorService samplers = Executors.newScheduledThreadPool(2, Thread.ofPlatform().daemon().name("samplers").factory());
            List<RunRecorder.ProducerSample> producerSamples = new ArrayList<>();
            List<RunRecorder.LagSample> lagSamples = new ArrayList<>();
            AtomicLong zeroLagStreak = new AtomicLong();

            long startNanos = System.nanoTime() + TimeUnit.SECONDS.toNanos(1);
            long startUs = nowMicros() + 1_000_000;
            long measureStartUs = startUs + config.warmupSeconds() * 1_000_000L;
            long measureEndUs = measureStartUs + config.durationSeconds() * 1_000_000L;
            recorder.markStarted(startUs, measureStartUs, measureEndUs);
            recorder.event(startUs, "WARMUP_START", Map.of("rate", config.eventRate()));
            recorder.event(measureStartUs, "MEASURE_START", Map.of());
            recorder.event(measureEndUs, "MEASURE_END", Map.of());

            long[] previous = new long[3];
            samplers.scheduleAtFixedRate(() -> {
                long s = sent.get();
                long a = acked.get();
                long f = failed.get();
                double elapsed = (System.nanoTime() - startNanos) / 1e9;
                String phase = elapsed < config.warmupSeconds() ? "warmup" : elapsed < config.warmupSeconds() + config.durationSeconds() ? "measure" : "drain";
                synchronized (producerSamples) {
                    producerSamples.add(new RunRecorder.ProducerSample(nowMicros(), phase, plan.requestedEventRate(Math.max(0, elapsed)),
                            s - previous[0], a - previous[1], f - previous[2], intervalMaxScheduleLagMs.getAndSet(0)));
                }
                previous[0] = s;
                previous[1] = a;
                previous[2] = f;
            }, 1, 1, TimeUnit.SECONDS);
            samplers.scheduleWithFixedDelay(() -> {
                try {
                    List<RunRecorder.LagSample> batch = lagMonitor.sample();
                    boolean zero = batch.stream().allMatch(sample -> sample.lag() == 0);
                    zeroLagStreak.set(zero ? zeroLagStreak.get() + 1 : 0);
                    synchronized (lagSamples) {
                        lagSamples.addAll(batch);
                    }
                } catch (Exception e) {
                    LOG.debugf("Lag sample failed: %s", e.toString());
                }
            }, 0, config.lagSampleIntervalMs(), TimeUnit.MILLISECONDS);
            samplers.scheduleWithFixedDelay(() -> flushSamples(recorder, producerSamples, lagSamples), 10, 10, TimeUnit.SECONDS);

            sendAll(plan, producer, recorder, startNanos);
            producer.flush();
            long sendEndUs = nowMicros();
            recorder.event(sendEndUs, "SEND_END", Map.of("sent", sent.get(), "acked", acked.get(), "failed", failed.get()));
            double activeSeconds = (sendEndUs - startUs) / 1e6;
            double achievedRate = acked.get() / Math.max(1e-9, activeSeconds);
            LOG.infof("All events sent: sent=%d acked=%d failed=%d maxScheduleLagMs=%d achieved=%.0f/s",
                    sent.get(), acked.get(), failed.get(), maxScheduleLagMs.get(), achievedRate);

            zeroLagStreak.set(0);
            long drainDeadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(config.drainTimeoutSeconds());
            while (zeroLagStreak.get() < config.drainZeroSamples() && System.nanoTime() < drainDeadline) {
                Thread.sleep(250);
            }
            boolean drained = zeroLagStreak.get() >= config.drainZeroSamples();
            long finishedUs = nowMicros();
            recorder.event(finishedUs, drained ? "DRAINED" : "DRAIN_TIMEOUT", Map.of());
            samplers.shutdown();
            samplers.awaitTermination(15, TimeUnit.SECONDS);
            flushSamples(recorder, producerSamples, lagSamples);
            recorder.finish(sendEndUs, finishedUs, sent.get(), acked.get(), failed.get(), achievedRate,
                    maxScheduleLagMs.get(), drained, drained ? "COMPLETED" : "DRAIN_TIMEOUT");
            LOG.infof("Run %s finished: drained=%s", config.runId(), drained);
            return drained && failed.get() == 0 ? 0 : 2;
        }
    }

    private void sendAll(WorkloadPlan plan, KafkaProducer<String, byte[]> producer, RunRecorder recorder, long startNanos) {
        Counter sentCounter = Counter.builder("bench_generator_sent_total").register(registry);
        PriorityQueue<Scheduled> queue = new PriorityQueue<>((x, y) -> x.dueNanos() != y.dueNanos()
                ? Long.compare(x.dueNanos(), y.dueNanos()) : Long.compare(x.seq(), y.seq()));
        long nextAggregate = 0;
        long seq = 0;
        long total = plan.totalAggregates();
        long nextAggregateDue = startNanos + plan.startUs(0) * 1000;
        while (nextAggregate < total || !queue.isEmpty()) {
            long now = System.nanoTime();
            while (nextAggregate < total && nextAggregateDue <= now) {
                WorkloadPlan.AggregatePlan aggregate = plan.plan(nextAggregate);
                long aggregateStart = startNanos + aggregate.startUs() * 1000;
                for (WorkloadPlan.PlannedEvent event : aggregate.events()) {
                    queue.add(new Scheduled(aggregateStart + event.offsetUs() * 1000, seq++, aggregate, event));
                }
                nextAggregate++;
                nextAggregateDue = nextAggregate < total ? startNanos + plan.startUs(nextAggregate) * 1000 : Long.MAX_VALUE;
            }
            while (!queue.isEmpty() && queue.peek().dueNanos() <= now) {
                Scheduled next = queue.poll();
                long lagMs = TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - next.dueNanos());
                intervalMaxScheduleLagMs.accumulateAndGet(lagMs, Math::max);
                maxScheduleLagMs.accumulateAndGet(lagMs, Math::max);
                send(plan, producer, recorder, next);
                sentCounter.increment();
                now = System.nanoTime();
            }
            long wakeAt = Math.min(nextAggregateDue, queue.isEmpty() ? Long.MAX_VALUE : queue.peek().dueNanos());
            long sleep = Math.min(wakeAt - System.nanoTime(), 1_000_000);
            if (sleep > 50_000) {
                LockSupport.parkNanos(sleep);
            }
        }
    }

    private void send(WorkloadPlan plan, KafkaProducer<String, byte[]> producer, RunRecorder recorder, Scheduled scheduled) {
        FragmentEvent event = plan.event(config.runId(), scheduled.plan(), scheduled.event(), nowMicros());
        ProducerRecord<String, byte[]> record = new ProducerRecord<>(config.ingressTopic(), scheduled.plan().partition(),
                event.correlationId(), Json.toBytes(event));
        sent.incrementAndGet();
        producer.send(record, (metadata, error) -> {
            if (error == null) {
                acked.incrementAndGet();
            } else {
                failed.incrementAndGet();
                try {
                    recorder.insertSendFailure(event.correlationId(), event.eventId(), error.toString());
                } catch (Exception e) {
                    LOG.error("Could not record send failure", e);
                }
            }
        });
    }

    private static void flushSamples(RunRecorder recorder, List<RunRecorder.ProducerSample> producerSamples,
                                     List<RunRecorder.LagSample> lagSamples) {
        try {
            List<RunRecorder.ProducerSample> p;
            synchronized (producerSamples) {
                p = new ArrayList<>(producerSamples);
                producerSamples.clear();
            }
            List<RunRecorder.LagSample> l;
            synchronized (lagSamples) {
                l = new ArrayList<>(lagSamples);
                lagSamples.clear();
            }
            recorder.insertProducerSamples(p);
            recorder.insertLagSamples(l);
        } catch (Exception e) {
            LOG.warn("Could not store samples", e);
        }
    }

    private Properties producerProperties() {
        Properties props = new Properties();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, config.bootstrapServers());
        props.put(ProducerConfig.CLIENT_ID_CONFIG, "load-generator-" + config.runId());
        props.put(ProducerConfig.ACKS_CONFIG, "all");
        props.put(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG, true);
        props.put(ProducerConfig.LINGER_MS_CONFIG, 2);
        props.put(ProducerConfig.BATCH_SIZE_CONFIG, 65_536);
        props.put(ProducerConfig.COMPRESSION_TYPE_CONFIG, "lz4");
        props.put(ProducerConfig.BUFFER_MEMORY_CONFIG, 128L * 1024 * 1024);
        props.put(ProducerConfig.DELIVERY_TIMEOUT_MS_CONFIG, 300_000);
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, ByteArraySerializer.class);
        return props;
    }

    private Map<String, Object> configSnapshot() {
        Map<String, Object> snapshot = new LinkedHashMap<>();
        WorkloadPlan.Settings s = config.planSettings();
        snapshot.put("plan", s);
        snapshot.put("ingressTopic", config.ingressTopic());
        snapshot.put("lagGroups", config.lagGroups());
        snapshot.put("variant", config.variant());
        snapshot.put("scenario", config.scenario());
        return snapshot;
    }

    static List<LagMonitor.Target> parseTargets(String spec) {
        List<LagMonitor.Target> targets = new ArrayList<>();
        for (String pair : spec.split(",")) {
            String[] parts = pair.trim().split(":");
            targets.add(new LagMonitor.Target(parts[0], parts[1]));
        }
        return targets;
    }
}
