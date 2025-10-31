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

/**
 * Server demo for Backpressure / Buffer overflow.
 *
 * Key ideas:
 * - Use an explicit ArrayBlockingQueue as the inbound buffer so we can observe queue.size().
 * - MQTT callback offers messages into the queue (non-blocking offer).
 * - If offer fails => overflow event (count & broadcast).
 * - A separate consumer thread / Flowable reads from the queue (blocking take) and emits to Rx pipeline.
 * - Rx pipeline window(5s) -> stats, and we broadcast stats via WebSocket.
 *
 * This design is explicit and great for demoing buffer fullness and overflow.
 */
public class Server {
    // Configurable parameters for demo
    private static final int BUFFER_CAPACITY = 3000;       // capacity of inbound queue
    private static final int THROTTLE_THRESHOLD = BUFFER_CAPACITY*80/100;   // queue.size() >= this => throttled
    private static final long QUEUE_OFFER_TIMEOUT_MS = 50; // try offer this long before consider drop

    private final BroadcastWebSocket wsServer;
    private final MqttClient mqttClient;

    // inbound queue = actual server-side buffer we can query
    private final ArrayBlockingQueue<Double> inboundQueue = new ArrayBlockingQueue<>(BUFFER_CAPACITY);

    // counters
    private final AtomicInteger overflowCount = new AtomicInteger(0);

    // executor for queue consumer thread
    private final ExecutorService consumerExecutor = Executors.newSingleThreadExecutor(r -> {
        Thread t = new Thread(r, "queue-consumer-thread");
        t.setDaemon(true);
        return t;
    });

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

        // MQTT callback: parse and offer into inboundQueue (non-blocking offer with tiny timeout)
        mqttClient.setCallback(new MqttCallback() {
            @Override
            public void connectionLost(Throwable cause) {
                System.err.println("⚠️ MQTT connection lost: " + cause.getMessage());
            }

            @Override
            public void messageArrived(String topic, MqttMessage message) {
                try {
                    String payload = new String(message.getPayload(), StandardCharsets.UTF_8);
                    JsonObject json = JsonParser.parseString(payload).getAsJsonObject();
                    double temp = json.get("temperature").getAsDouble();
                    long time = json.has("time") ? json.get("time").getAsLong() : Instant.now().toEpochMilli();

                    System.out.printf("📩 Received (MQTT): temperature=%.2f°C time=%d%n", temp, time);

                    // Immediately update current reading to clients
                    JsonObject out = new JsonObject();
                    out.addProperty("type", "current");
                    out.addProperty("temperature", temp);
                    out.addProperty("time", time);
                    wsServer.broadcastJson(out);

                    // Try to insert into inboundQueue within a short timeout
                    boolean offered = inboundQueue.offer(temp);
                    if (!offered) {
                        int o = overflowCount.incrementAndGet();
                        System.err.println("🔥 Queue overflow! sample dropped. totalOverflows=" + o);

                        JsonObject evt = new JsonObject();
                        evt.addProperty("type", "overflow");
                        evt.addProperty("message", "Queue full - sample dropped");
                        evt.addProperty("totalOverflows", o);
                        evt.addProperty("timestamp", System.currentTimeMillis());
                        wsServer.broadcastJson(evt);
                    }
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

    public void start() {
        wsServer.start();
        System.out.println("🌐 WebSocket server started on port " + wsServer.getPort());

        // Create a Flowable that emits values read from the inboundQueue (blocking take).
        Flowable<Double> stream = Flowable.create(emitter -> {
            // run on dedicated thread
            consumerExecutor.submit(() -> {
                try {
                    while (!Thread.currentThread().isInterrupted() && !emitter.isCancelled()) {
                        Double v = inboundQueue.take(); // blocks until available
                        Thread.sleep(5);
                        emitter.onNext(v);
                    }
                } catch (InterruptedException ie) {
                    emitter.onComplete();
                } catch (Throwable t) {
                    emitter.onError(t);
                }
            });
        }, BackpressureStrategy.MISSING); // queue handles buffering

        // Rx pipeline: window 5s -> collect -> compute stats -> broadcast
        stream
                .observeOn(Schedulers.computation())
                .window(5, TimeUnit.SECONDS)
                .flatMapSingle(window -> window.collect(ArrayList<Double>::new, List::add))
                .filter(list -> !list.isEmpty())
                .observeOn(Schedulers.computation())
                .subscribe(list -> {
                    // Simulate slow processing to create backlog (for demo). Adjust or remove in prod.

                    double avg = list.stream().mapToDouble(Double::doubleValue).average().orElse(0);
                    double max = list.stream().mapToDouble(Double::doubleValue).max().orElse(0);
                    double min = list.stream().mapToDouble(Double::doubleValue).min().orElse(0);
                    double last = list.get(list.size() - 1);
                    double fluctuation = max - min;
                    double rate = (last - list.get(0)) / 5.0;
                    int count = list.size();
                    double throughput = count / 5.0;
                    boolean alert = avg > 30 || avg < 15;

                    // ** Real buffer size we can observe **
                    int bufferNow = inboundQueue.size();
                    boolean throttled = bufferNow >= THROTTLE_THRESHOLD;

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
                    stats.addProperty("throttled", throttled);
                    stats.addProperty("bufferSize", bufferNow);
                    stats.addProperty("bufferCapacity", BUFFER_CAPACITY);
                    stats.addProperty("totalOverflows", overflowCount.get());
                    stats.addProperty("timestamp", System.currentTimeMillis());

                    wsServer.broadcastJson(stats);

                    System.out.printf("📊 [5s Stats] avg=%.2f°C max=%.2f°C min=%.2f°C Δ=%.2f°C rate=%.2f°C/s count=%d buffer=%d/%d throttled=%b overflows=%d%n",
                            avg, max, min, fluctuation, rate, count, bufferNow, BUFFER_CAPACITY, throttled, overflowCount.get());
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
