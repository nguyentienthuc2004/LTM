package org.example;

import com.google.gson.JsonObject;
import com.google.gson.JsonParser;
import io.reactivex.rxjava3.core.Flowable;
import io.reactivex.rxjava3.core.BackpressureStrategy;
import io.reactivex.rxjava3.schedulers.Schedulers;
import org.eclipse.paho.client.mqttv3.*;
import org.eclipse.paho.client.mqttv3.persist.MemoryPersistence;

import java.net.InetSocketAddress;
import java.nio.charset.StandardCharsets;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.ThreadLocalRandom;

/**
 * Server demo for Backpressure / Buffer overflow.
 *
 * Improvements:
 * - Offload all WebSocket broadcast + control publish into broadcastExecutor
 *   so messageArrived (MQTT callback thread) is never blocked by slow WS clients.
 * - messageArrived now does: parse -> quick log -> enqueue -> submit snapshot task.
 * - Control (ltm/slow, ltm/normal) publishing is also done from broadcastExecutor.
 */
public class Server {
    // Configurable parameters
    private static final int BUFFER_CAPACITY = 4000;
    private static final int THROTTLE_THRESHOLD = BUFFER_CAPACITY * 80 / 100;
    private static final int RESUME_THRESHOLD = BUFFER_CAPACITY * 40 / 100;
    private static final long QUEUE_OFFER_TIMEOUT_MS = 50;
    private static final long WINDOW_SECONDS=5;
    private static final long minTemp = 15;
    private static final long maxTemp = 30;
    private final BroadcastWebSocket wsServer;
    private final MqttClient mqttClient;

    // inbound queue
    private final ArrayBlockingQueue<Double> inboundQueue = new ArrayBlockingQueue<>(BUFFER_CAPACITY);

    // counters
    private final AtomicInteger overflowCount = new AtomicInteger(0);

    // throttled state: 0 = normal, 1 = throttled
    private final AtomicInteger throttledState = new AtomicInteger(0);

    // executor for consumer
    private final ExecutorService consumerExecutor = Executors.newSingleThreadExecutor(r -> {
        Thread t = new Thread(r, "queue-consumer-thread");
        t.setDaemon(true);
        return t;
    });

    // executor to offload broadcasts & control publishes (prevents blocking MQTT callback)
    private final ExecutorService broadcastExecutor = Executors.newSingleThreadExecutor(r -> {
        Thread t = new Thread(r, "broadcast-executor");
        t.setDaemon(true);
        return t;
    });

    // control topics
    private static final String SLOW_TOPIC = "ltm/slow";
    private static final String NORMAL_TOPIC = "ltm/normal";

    public Server(int wsPort, String mqttUrl, String mqttUser, String mqttPass) throws MqttException {
        wsServer = new BroadcastWebSocket(new InetSocketAddress(wsPort));

        mqttClient = new MqttClient(
                mqttUrl,
                "temp-server-" + ThreadLocalRandom.current().nextInt(10000),
                new MemoryPersistence()
        );

        MqttConnectOptions opts = new MqttConnectOptions();
        opts.setAutomaticReconnect(true);
        opts.setCleanSession(true);
        opts.setUserName(mqttUser);
        opts.setPassword(mqttPass.toCharArray());

        // MQTT callback
        mqttClient.setCallback(new MqttCallback() {
            @Override
            public void connectionLost(Throwable cause) {
                System.err.println("⚠️ MQTT connection lost: " + (cause != null ? cause.getMessage() : "unknown"));
                // Offload WS broadcast to avoid blocking MQTT thread
                broadcastExecutor.execute(() -> {
                    JsonObject s = new JsonObject();
                    s.addProperty("type", "system");
                    s.addProperty("systemStatus", "MQTT_DISCONNECTED");
                    s.addProperty("message", "MQTT connection lost");
                    s.addProperty("timestamp", System.currentTimeMillis());
                    wsServer.broadcastJson(s);
                });
            }

            @Override
            public void messageArrived(String topic, MqttMessage message) {
                try {
                    // chỉ xử lý topic "ltm"
                    if (!"ltm".equals(topic)) return;

                    String payload = new String(message.getPayload(), StandardCharsets.UTF_8);
                    JsonObject json = JsonParser.parseString(payload).getAsJsonObject();
                    if (!json.has("temperature")) return;

                    double temp = json.get("temperature").getAsDouble();
                    long time = json.has("time") ? json.get("time").getAsLong() : Instant.now().toEpochMilli();

                    // QUICK: in log (cheap) — so you always see realtime current in console
                    System.out.printf("📩 [CURRENT] temperature=%.2f°C time=%d systemStatus=%s buffer=%d/%d overflows=%d%n",
                            temp, time, throttledState.get() == 1 ? "PAUSED" : "RUNNING",
                            inboundQueue.size(), BUFFER_CAPACITY, overflowCount.get());

                    // QUICK: build minimal current object (we won't block on broadcasting)
                    JsonObject current = new JsonObject();
                    current.addProperty("type", "current");
                    current.addProperty("temperature", temp);
                    current.addProperty("time", time);
                    current.addProperty("systemStatus", throttledState.get() == 1 ? "PAUSED" : "RUNNING");

                    // Try to insert into queue quickly (non-blocking)
                    boolean offered = inboundQueue.offer(temp);
                    if (!offered) {
                        int o = overflowCount.incrementAndGet();
                        System.err.println("🔥 Queue overflow! sample dropped. totalOverflows=" + o);

                        // Build overflow event
                        JsonObject evt = new JsonObject();
                        evt.addProperty("type", "overflow");
                        evt.addProperty("message", "Queue full - sample dropped");
                        evt.addProperty("totalOverflows", o);
                        evt.addProperty("bufferSize", inboundQueue.size());
                        evt.addProperty("bufferCapacity", BUFFER_CAPACITY);
                        evt.addProperty("timestamp", System.currentTimeMillis());
                        evt.addProperty("systemStatus", throttledState.get() == 1 ? "PAUSED" : "RUNNING");

                        // Offload broadcast of overflow and a snapshot so MQTT thread is not blocked
                        broadcastExecutor.execute(() -> {
                            wsServer.broadcastJson(evt);
                            JsonObject snapO = buildSystemSnapshot();
                            wsServer.broadcastJson(snapO);
                        });
                        return;
                    }

                    // If enqueued successfully: check buffer size immediately and possibly trigger throttling.
                    int bufferNow = inboundQueue.size();
                    boolean nowThrottled = bufferNow >= THROTTLE_THRESHOLD;
                    if (nowThrottled && throttledState.compareAndSet(0, 1)) {
                        // Offload control publish + broadcast
                        JsonObject ctrl = new JsonObject();
                        ctrl.addProperty("type", "slow");
                        ctrl.addProperty("systemStatus", "PAUSED");
                        ctrl.addProperty("buffer", bufferNow);
                        ctrl.addProperty("capacity", BUFFER_CAPACITY);
                        ctrl.addProperty("timestamp", System.currentTimeMillis());
                        ctrl.addProperty("message", "Buffer high - please slow down sending");

                        broadcastExecutor.execute(() -> {
                            try {
                                mqttClient.publish(SLOW_TOPIC, ctrl.toString().getBytes(StandardCharsets.UTF_8), 1, false);
                            } catch (MqttException me) {
                                System.err.println("❌ Failed to publish SLOW control: " + me.getMessage());
                            }
                            wsServer.broadcastJson(ctrl);
                            System.out.printf("📡 Published %s (buffer=%d/%d)%n", SLOW_TOPIC, bufferNow, BUFFER_CAPACITY);
                        });
                    }

                    // Finally: offload broadcasting the current reading + realtime snapshot (lightweight task)
                    broadcastExecutor.execute(() -> {
                        wsServer.broadcastJson(current);
                        JsonObject snap = buildSystemSnapshot();
                        wsServer.broadcastJson(snap);
                    });

                } catch (Exception e) {
                    System.err.println("❌ Failed to parse MQTT message: " + e);
                }
            }

            @Override
            public void deliveryComplete(IMqttDeliveryToken token) {}
        });

        // connect + subscribe
        mqttClient.connect(opts);
        mqttClient.subscribe("ltm", 0);
        System.out.println("✅ Connected to MQTT broker and subscribed to topic 'ltm'");
    }

    private JsonObject buildSystemSnapshot() {
        JsonObject snap = new JsonObject();
        snap.addProperty("type", "system");
        snap.addProperty("systemStatus", throttledState.get() == 1 ? "PAUSED" : "RUNNING");
        snap.addProperty("throttled", throttledState.get() == 1);
        snap.addProperty("bufferSize", inboundQueue.size());
        snap.addProperty("bufferCapacity", BUFFER_CAPACITY);
        snap.addProperty("totalOverflows", overflowCount.get());
        snap.addProperty("timestamp", System.currentTimeMillis());
        return snap;
    }

    public void start() {
        wsServer.start();
        System.out.println("🌐 WebSocket server started on port " + wsServer.getPort());

        // Consumer thread -> Flowable
        Flowable<Double> stream = Flowable.create(emitter -> {
            consumerExecutor.submit(() -> {
                try {
                    while (!Thread.currentThread().isInterrupted() && !emitter.isCancelled()) {
                        Double v = inboundQueue.take(); // blocks
                        Thread.sleep(5);
                        // simulate processing
                        emitter.onNext(v);

                        // after consuming, check resume condition
                        int bufferNow = inboundQueue.size();
                        if (throttledState.get() == 1 && bufferNow < RESUME_THRESHOLD && throttledState.compareAndSet(1, 0)) {
                            // publish NORMAL and broadcast snapshot (offloaded)
                            JsonObject ctrl = new JsonObject();
                            ctrl.addProperty("type", "normal");
                            ctrl.addProperty("systemStatus", "RUNNING");
                            ctrl.addProperty("buffer", bufferNow);
                            ctrl.addProperty("capacity", BUFFER_CAPACITY);
                            ctrl.addProperty("timestamp", System.currentTimeMillis());
                            ctrl.addProperty("message", "Buffer dropped - resume normal sending");

                            broadcastExecutor.execute(() -> {
                                try {
                                    mqttClient.publish(NORMAL_TOPIC, ctrl.toString().getBytes(StandardCharsets.UTF_8), 1, false);
                                } catch (MqttException me) {
                                    System.err.println("❌ Failed to publish NORMAL control: " + me.getMessage());
                                }
                                wsServer.broadcastJson(ctrl);
                                System.out.printf("📡 Published %s (buffer=%d/%d)%n", NORMAL_TOPIC, bufferNow, BUFFER_CAPACITY);

                                // send realtime system snapshot on resume
                                JsonObject snap = buildSystemSnapshot();
                                wsServer.broadcastJson(snap);
                            });
                        }
                    }
                } catch (InterruptedException ie) {
                    emitter.onComplete();
                } catch (Throwable t) {
                    emitter.onError(t);
                }
            });
        }, BackpressureStrategy.MISSING);

        // keep 5s window stats for aggregated metrics (unchanged)
        stream
                .observeOn(Schedulers.computation())
                .window(WINDOW_SECONDS, TimeUnit.SECONDS)
                .flatMapSingle(window -> window.collect(ArrayList<Double>::new, List::add))
                .filter(list -> !list.isEmpty())
                .observeOn(Schedulers.computation())
                .subscribe(list -> {
                    // compute stats
                    double avg = list.stream().mapToDouble(Double::doubleValue).average().orElse(0);
                    double max = list.stream().mapToDouble(Double::doubleValue).max().orElse(0);
                    double min = list.stream().mapToDouble(Double::doubleValue).min().orElse(0);
                    double last = list.get(list.size() - 1);
                    double fluctuation = max - min;
                    double rate = (last - list.get(0)) / WINDOW_SECONDS;
                    int count = list.size();
                    double throughput = count / WINDOW_SECONDS;
                    boolean alert = avg > maxTemp || avg < minTemp;

                    // realtime fields (take current state immediately)
                    int bufferNow = inboundQueue.size();
                    boolean throttled = throttledState.get() == 1;
                    int totalOver = overflowCount.get();

                    JsonObject stats = new JsonObject();
                    stats.addProperty("type", "stats");
                    stats.addProperty("avg", avg);
                    stats.addProperty("max", max);
                    stats.addProperty("min", min);
                    stats.addProperty("current", last);
                    stats.addProperty("fluctuation", fluctuation);
                    stats.addProperty("rate", rate);
                    stats.addProperty("count", count);
                    stats.addProperty("throughput", throughput);
                    stats.addProperty("alert", alert);

                    // realtime values included
                    stats.addProperty("throttled", throttled);
                    stats.addProperty("bufferSize", bufferNow);
                    stats.addProperty("bufferCapacity", BUFFER_CAPACITY);
                    stats.addProperty("totalOverflows", totalOver);
                    stats.addProperty("timestamp", System.currentTimeMillis());

                    wsServer.broadcastJson(stats);

                    System.out.printf("📊 [5s Stats] avg=%.2f°C max=%.2f°C min=%.2f°C Δ=%.2f°C rate=%.2f°C/s count=%d buffer=%d/%d throttled=%b overflows=%d%n",
                            avg, max, min, fluctuation, rate, count, bufferNow, BUFFER_CAPACITY, throttled, totalOver);
                }, err -> {
                    System.err.println("❌ Stream error: " + err);
                    err.printStackTrace();
                });
    }

    public static void main(String[] args) throws Exception {
        Server server = new Server(
                8884,
                "ssl://8e35c2a9c7114b3f9649e7a3e2e982e3.s1.eu.hivemq.cloud:8883",
                "thucng04",
                "Anhthucdz1"
        );
        server.start();
        System.out.println("🚀 Server running. Press Ctrl+C to stop.");
    }
}
