package org.example;

import com.google.gson.JsonObject;
import org.eclipse.paho.client.mqttv3.*;
import org.eclipse.paho.client.mqttv3.persist.MemoryPersistence;
import org.eclipse.paho.client.mqttv3.MqttCallbackExtended;

import javax.net.ssl.SSLSocketFactory;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.atomic.AtomicInteger;

public class Client {

    public static void main(String[] args) throws Exception {
        String broker = "ssl://8e35c2a9c7114b3f9649e7a3e2e982e3.s1.eu.hivemq.cloud:8883";
        String username = "thucng04";
        String password = "Anhthucdz1";

        String dataTopic = "ltm";
        String slowTopic = "ltm/slow";      // <- subscribe topic mới khi server báo throttled
        String normalTopic = "ltm/normal";  // <- subscribe topic mới khi server báo bình thường

        // delay mặc định và delay khi server báo quá tải
        final int NORMAL_DELAY = 1;    // ms giữa các mẫu khi bình thường (giữ theo code cũ)
        final int SLOW_DELAY = 1000;   // ms giữa các mẫu khi server yêu cầu chậm lại

        AtomicInteger sendDelay = new AtomicInteger(NORMAL_DELAY);

        // Tạo MQTT client
        String clientId = MqttClient.generateClientId();
        MqttClient mqttClient = new MqttClient(broker, clientId, new MemoryPersistence());

        MqttConnectOptions options = new MqttConnectOptions();
        options.setUserName(username);
        options.setPassword(password.toCharArray());
        options.setSocketFactory(SSLSocketFactory.getDefault());
        options.setAutomaticReconnect(true);
        options.setCleanSession(true);

        // Callback extended để tự động subscribe lại sau reconnect
        mqttClient.setCallback(new MqttCallbackExtended() {
            @Override
            public void connectComplete(boolean reconnect, String serverURI) {
                System.out.println("✅ MQTT connected (reconnect=" + reconnect + ") to " + serverURI);
                try {
                    // đảm bảo subscribe control topics sau mỗi kết nối
                    mqttClient.subscribe(slowTopic, 1);
                    mqttClient.subscribe(normalTopic, 1);
                    System.out.println("📡 Subscribed to control topics: " + slowTopic + " , " + normalTopic);
                } catch (MqttException e) {
                    System.err.println("❌ Failed to (re)subscribe control topics: " + e.getMessage());
                }
            }

            @Override
            public void connectionLost(Throwable cause) {
                System.err.println("⚠️ MQTT connection lost: " + (cause != null ? cause.getMessage() : "unknown"));
            }

            @Override
            public void messageArrived(String topic, MqttMessage message) {
                String msg = new String(message.getPayload());
                // nhận control từ server, set delay tương ứng
                if (topic.equals(slowTopic)) {
                    sendDelay.set(SLOW_DELAY);
                    System.out.println("🐢 RECEIVED SLOW -> giảm tốc gửi: " + SLOW_DELAY + " ms/mẫu");
                } else if (topic.equals(normalTopic)) {
                    sendDelay.set(NORMAL_DELAY);
                    System.out.println("🚀 RECEIVED NORMAL -> gửi nhanh lại: " + NORMAL_DELAY + " ms/mẫu");
                } else {
                    // không phải control topic
                    System.out.println("📩 Control khác: " + topic + " = " + msg);
                }
            }

            @Override
            public void deliveryComplete(IMqttDeliveryToken token) {
                // không cần hành động gì
            }
        });

        // Kết nối (blocking). Automatic reconnect sẽ cố gắng lại nếu sau này mất kết nối.
        mqttClient.connect(options);

        // Sau kết nối ban đầu, subscribe control topics (connectComplete cũng sẽ re-subscribe sau reconnect)
        mqttClient.subscribe(slowTopic, 1);
        mqttClient.subscribe(normalTopic, 1);
        System.out.println("📡 Đã subscribe các topic điều khiển: " + slowTopic + " , " + normalTopic);

        // Giả lập dữ liệu cảm biến
        double baseTemp = 25.0;
        double currentTemp = baseTemp;
        ThreadLocalRandom rnd = ThreadLocalRandom.current();

        // Vòng lặp gửi dữ liệu
        while (true) {
            // Nếu mất kết nối, chờ và tiếp tục (tránh throw khi publish)
            if (!mqttClient.isConnected()) {
                // đợi một chút trước khi thử lại gửi
                Thread.sleep(200);
                continue;
            }

            // Tạo dao động nhỏ ±0.5°C
            double variation = rnd.nextDouble(-0.5, 0.5);
            currentTemp += variation;
            if (currentTemp < 20) currentTemp = 20;
            if (currentTemp > 35) currentTemp = 35;

            // 3% cơ hội xảy ra spike
            if (rnd.nextInt(0, 35) == 0) {
                double spike = rnd.nextDouble(2, 5);
                currentTemp += rnd.nextBoolean() ? spike : -spike;
                System.out.println("⚠️ Spike event!");
            }

            // Tạo payload JSON
            JsonObject payload = new JsonObject();
            payload.addProperty("temperature", currentTemp);
            payload.addProperty("time", System.currentTimeMillis());

            MqttMessage message = new MqttMessage(payload.toString().getBytes());
            message.setQos(0);

            try {
                mqttClient.publish(dataTopic, message);
            } catch (MqttException me) {
                System.err.println("❌ Publish failed (will retry later): " + me.getMessage());
                // nếu publish fail do connection broken, vòng lặp sẽ detect mqttClient.isConnected() = false và chờ
            }

            // Logging nhỏ (có thể tắt nếu nhiều)
            System.out.println("📤 Sent (" + sendDelay.get() + " ms): " + payload);

            // Sleep theo delay hiện tại (điều chỉnh tốc độ gửi)
            Thread.sleep(sendDelay.get());
        }
    }
}
