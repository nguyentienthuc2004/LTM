package org.example;

import com.google.gson.JsonObject;
import com.google.gson.JsonParser;
import io.reactivex.rxjava3.core.BackpressureOverflowStrategy;
import io.reactivex.rxjava3.core.Flowable;
import io.reactivex.rxjava3.processors.BehaviorProcessor;
import io.reactivex.rxjava3.processors.PublishProcessor;
import io.reactivex.rxjava3.schedulers.Schedulers;
import org.eclipse.paho.client.mqttv3.*;
import org.eclipse.paho.client.mqttv3.persist.MemoryPersistence;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.InetSocketAddress;
import java.nio.charset.StandardCharsets;
import java.time.Instant;
import java.util.Set;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.logging.Level;

public class Server {
    private static final Logger log = LoggerFactory.getLogger(Server.class);

    // RxJava processor nhận dữ liệu trade từ MQTT
    private final PublishProcessor<TradeData> source = PublishProcessor.create();

    // Processor dùng để điều khiển tốc độ phát dữ liệu (throttling)
    private final BehaviorProcessor<Long> throttlePeriodMs = BehaviorProcessor.createDefault(1000L);

    // AtomicInteger dùng để đếm số trade đang chờ xử lý
    private final AtomicInteger pendingCount = new AtomicInteger(0);

    // Đếm số lần buffer bị tràn
    private final AtomicInteger overflowCount = new AtomicInteger(0);

    // Lưu trữ các client WebSocket đang kết nối (hiện tại chưa dùng)
    private final Set<String> connectedClients = ConcurrentHashMap.newKeySet();

    // WebSocket server
    private final BroadcastWebSocket wsServer;

    // MQTT client
    private final MqttClient mqttClient;

    // Scheduler để chạy task adaptive control
    private final ScheduledExecutorService controlSched = Executors.newSingleThreadScheduledExecutor();

    /**
     * Constructor server
     */
    public Server(int wsPort, String mqttUrl, String mqttUser, String mqttPass) throws MqttException {
        // Khởi tạo WebSocket server
        wsServer = new BroadcastWebSocket(new InetSocketAddress(wsPort));

        // Khởi tạo MQTT client với clientId ngẫu nhiên
        mqttClient = new MqttClient(
                mqttUrl,
                "rxtrade-server-" + ThreadLocalRandom.current().nextInt(10000),
                new MemoryPersistence()
        );

        // Cấu hình MQTT
        MqttConnectOptions opts = new MqttConnectOptions();
        opts.setAutomaticReconnect(true); // tự động reconnect khi mất kết nối
        opts.setCleanSession(true);       // clean session
        opts.setUserName(mqttUser);
        opts.setPassword(mqttPass.toCharArray());

        // Callback MQTT
        mqttClient.setCallback(new MqttCallback() {
            @Override
            public void connectionLost(Throwable cause) {
                log.debug("MQTT connection lost: " + cause);
            }

            @Override
            public void messageArrived(String topic, MqttMessage message) {
                try {
                    // Chuyển payload sang JSON
                    String payload = new String(message.getPayload(), StandardCharsets.UTF_8);
                    JsonObject json = JsonParser.parseString(payload).getAsJsonObject();

                    double price = json.get("price").getAsDouble();
                    double quantity = json.get("quantity").getAsDouble();
                    // Nếu JSON không có time thì dùng thời gian hiện tại
                    long time = json.has("time") ? json.get("time").getAsLong() : Instant.now().toEpochMilli();

                    // Đẩy dữ liệu trade vào RxJava processor
                    source.onNext(new TradeData(price, quantity, time));
                } catch (Exception e) {
                    log.debug("Failed to parse MQTT message: " + e.getMessage());
                }
            }

            @Override
            public void deliveryComplete(IMqttDeliveryToken token) {}
        });

        // Kết nối MQTT broker và subscribe topic "ltm"
        mqttClient.connect(opts);
        mqttClient.subscribe("ltm", 0);
        log.debug("Connected to MQTT broker and subscribed to topic 'ltm'");
    }

    /**
     * Bắt đầu server
     */
    public void start() {
        // Khởi động WebSocket server
        wsServer.start();
        log.debug("WebSocket server started on port " + wsServer.getPort());

        // Buffer dữ liệu trade từ MQTT
        Flowable<TradeData> buffered = source
                .onBackpressureBuffer(
                        1000,                         // buffer tối đa 1000 trade
                        () -> overflowCount.incrementAndGet(), // nếu tràn thì tăng overflowCount
                        BackpressureOverflowStrategy.DROP_OLDEST // drop trade cũ nhất khi tràn
                ).share(); // share để nhiều subscriber có thể sử dụng cùng dữ liệu

        // Xử lý dữ liệu theo window 5 giây
        buffered.observeOn(Schedulers.computation())
                .doOnNext(d -> pendingCount.incrementAndGet()) // tăng pendingCount khi có trade mới
                .filter(TradeData::isValid)                    // lọc trade hợp lệ
                .window(5, TimeUnit.SECONDS)                  // gom window 5s
                .flatMapSingle(window -> window.collect(StatsCollector::new, StatsCollector::collect))
                .subscribe(wsServer::broadcastJson, err -> log.debug("Processing error", err));

        // Lấy mẫu dữ liệu theo throttlePeriodMs
        Flowable<TradeData> sampled = throttlePeriodMs.toSerialized()
                .switchMap(period -> buffered.sample(period, TimeUnit.MILLISECONDS));

        sampled.observeOn(Schedulers.io())
                .subscribe(d -> {
                    wsServer.broadcastJson(d);                   // phát dữ liệu ra WebSocket
                    pendingCount.updateAndGet(x -> Math.max(0, x - 1)); // giảm pendingCount
                }, err -> log.debug("Sampled error", err));

        // Lên lịch kiểm soát adaptive throttling mỗi giây
        controlSched.scheduleAtFixedRate(this::adaptiveControl, 1, 1, TimeUnit.SECONDS);
    }

    /**
     * Adaptive control: điều chỉnh tốc độ throttling dựa vào pendingCount
     */
    private void adaptiveControl() {
        int pending = pendingCount.get();
        long current = throttlePeriodMs.getValue();
        long newPeriod = current;

        // Điều chỉnh period dựa trên số lượng pending
        if (pending > 300) newPeriod = Math.min(10000L, current * 2);
        else if (pending > 100) newPeriod = Math.min(5000L, current + 500);
        else if (pending < 10) newPeriod = Math.max(200L, current / 2);

        // Nếu thay đổi period, phát ra BehaviorProcessor
        if (newPeriod != current) throttlePeriodMs.onNext(newPeriod);
    }

    /**
     * Dừng server
     */
    public void stop() throws Exception {
        controlSched.shutdownNow();             // dừng scheduler
        if (mqttClient != null) {
            mqttClient.disconnect();            // ngắt kết nối MQTT
            mqttClient.close();
        }
        wsServer.stop();                        // dừng WebSocket
    }

    /**
     * Chạy server
     */
    public static void main(String[] args) throws Exception {
        Server server = new Server(
                8884, // port WebSocket
                "ssl://0ac901938c97430b9bde1e30ea590141.s1.eu.hivemq.cloud:8883",
                "thucng04",
                "Anhthucdz1"
        );
        server.start();
        log.debug("Server running. Use Ctrl+C to exit.");
    }
}
