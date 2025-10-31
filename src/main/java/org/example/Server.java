package org.example;

import com.google.gson.JsonObject;
import com.google.gson.JsonParser;
import io.reactivex.rxjava3.core.Flowable;
import io.reactivex.rxjava3.processors.PublishProcessor;
import io.reactivex.rxjava3.schedulers.Schedulers;
import org.eclipse.paho.client.mqttv3.*;
import org.eclipse.paho.client.mqttv3.persist.MemoryPersistence;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.InetSocketAddress;
import java.nio.charset.StandardCharsets;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeUnit;

public class Server {
    private static final Logger log = LoggerFactory.getLogger(Server.class);

    // WebSocket server
    private final BroadcastWebSocket wsServer;

    // MQTT client
    private final MqttClient mqttClient;
    private final PublishProcessor<Double> stream = PublishProcessor.create();

    /**
     * Constructor server
     */
    public Server(int wsPort, String mqttUrl, String mqttUser, String mqttPass) throws MqttException {
        // Khởi tạo WebSocket server
        wsServer = new BroadcastWebSocket(new InetSocketAddress(wsPort));

        // Khởi tạo MQTT client
        mqttClient = new MqttClient(
                mqttUrl,
                "temp-server-" + ThreadLocalRandom.current().nextInt(10000),
                new MemoryPersistence()
        );

        // Cấu hình MQTT
        MqttConnectOptions opts = new MqttConnectOptions();
        opts.setAutomaticReconnect(true);
        opts.setCleanSession(true);
        opts.setUserName(mqttUser);
        opts.setPassword(mqttPass.toCharArray());

        // Callback nhận dữ liệu từ MQTT
        mqttClient.setCallback(new MqttCallback() {
            @Override
            public void connectionLost(Throwable cause) {
                log.warn("⚠️ MQTT connection lost: " + cause.getMessage());
            }

            @Override
            public void messageArrived(String topic, MqttMessage message) {
                try {
                    String payload = new String(message.getPayload(), StandardCharsets.UTF_8);
                    JsonObject json = JsonParser.parseString(payload).getAsJsonObject();

                    // ✅ Lấy dữ liệu nhiệt độ
                    double temp = json.get("temperature").getAsDouble();
                    long time = json.has("time") ? json.get("time").getAsLong() : Instant.now().toEpochMilli();

                    System.out.printf("📩 Received: temperature=%.2f°C time=%d%n", temp, time);

                    // Gửi ngay dữ liệu nhiệt độ hiện tại tới frontend
                    JsonObject out = new JsonObject();
                    out.addProperty("type", "current");
                    out.addProperty("temperature", temp);
                    out.addProperty("time", time);
                    wsServer.broadcastJson(out);

                    // Đưa vào luồng để xử lý thống kê
                    stream.onNext(temp);
                } catch (Exception e) {
                    log.error("❌ Failed to parse MQTT message: " + e.getMessage());
                }
            }

            @Override
            public void deliveryComplete(IMqttDeliveryToken token) {}
        });

        // Kết nối và đăng ký topic
        mqttClient.connect(opts);
        mqttClient.subscribe("ltm", 0);
        System.out.println("✅ Connected to MQTT broker and subscribed to topic 'ltm'");
    }

    /**
     * Xử lý dữ liệu thống kê 5 giây/lần
     */
    public void start() {
        wsServer.start();
        System.out.println("🌐 WebSocket server started on port " + wsServer.getPort());

        stream
                .window(5, TimeUnit.SECONDS)
                .flatMapSingle(window ->
                        window.collect(ArrayList<Double>::new, List::add)
                )
                .filter(list -> !list.isEmpty())
                .observeOn(Schedulers.computation())
                .subscribe(list -> {
                    double avg = list.stream().mapToDouble(Double::doubleValue).average().orElse(0);
                    double max = list.stream().mapToDouble(Double::doubleValue).max().orElse(0);
                    double min = list.stream().mapToDouble(Double::doubleValue).min().orElse(0);
                    double last = list.get(list.size() - 1);
                    double fluctuation = max - min;              // độ dao động
                    double rate = (last - list.get(0)) / 5.0;    // thay đổi trung bình °C/giây
                    int count = list.size();
                    double throughput = count / 5.0;             // số mẫu/giây
                    boolean alert = avg > 35 || avg < 15;        // cảnh báo nhiệt độ bất thường
                    boolean throttled = count > 50;              // mô phỏng quá tải dữ liệu

                    // Tạo JSON thống kê
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
                    stats.addProperty("timestamp", System.currentTimeMillis());

                    // Phát tới frontend
                    wsServer.broadcastJson(stats);

                    System.out.printf(
                            "📊 [5s Stats] avg=%.2f°C max=%.2f°C min=%.2f°C Δ=%.2f°C rate=%.2f°C/s count=%d alert=%b throttled=%b%n",
                            avg, max, min, fluctuation, rate, count, alert, throttled
                    );
                });
    }

    /**
     * Chạy server
     */
    public static void main(String[] args) throws Exception {
        Server server = new Server(
                8884, // WebSocket port
                "ssl://0ac901938c97430b9bde1e30ea590141.s1.eu.hivemq.cloud:8883",
                "thucng04",
                "Anhthucdz1"
        );
        server.start();
        System.out.println("🚀 Server running. Press Ctrl+C to stop.");
    }
}
