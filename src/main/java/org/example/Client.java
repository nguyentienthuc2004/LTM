package org.example;

import com.google.gson.JsonObject;
import org.eclipse.paho.client.mqttv3.*;
import javax.net.ssl.SSLSocketFactory;
import java.util.concurrent.ThreadLocalRandom;

public class Client {
    public static void main(String[] args) throws Exception {
        String broker = "ssl://8e35c2a9c7114b3f9649e7a3e2e982e3.s1.eu.hivemq.cloud:8883";
        String username = "thucng04";
        String password = "Anhthucdz1";
        String topic = "ltm";

        // Tạo MQTT client
        MqttClient mqttClient = new MqttClient(broker, MqttClient.generateClientId());
        MqttConnectOptions options = new MqttConnectOptions();
        options.setUserName(username);
        options.setPassword(password.toCharArray());
        options.setSocketFactory(SSLSocketFactory.getDefault());
        options.setAutomaticReconnect(true);

        mqttClient.connect(options);
        System.out.println("✅ Connected to HiveMQ Cloud");

        double baseTemp = 25.0; // Nhiệt độ trung bình ban đầu (°C)
        double currentTemp = baseTemp;

        while (true) {
            ThreadLocalRandom rnd = ThreadLocalRandom.current();

            // Dao động nhẹ (±0.5°C)
            double variation = rnd.nextDouble(-0.5, 0.5);
            currentTemp += variation;

            // Giới hạn trong khoảng hợp lý
            if (currentTemp < 20) currentTemp = 20;
            if (currentTemp > 35) currentTemp = 35;

            // Giả lập spike: 3% khả năng tăng/giảm mạnh
            if (rnd.nextInt(0, 35) == 0) {
                double spike = rnd.nextDouble(2, 5);
                currentTemp += rnd.nextBoolean() ? spike : -spike;
                System.out.println("⚠️ Spike event!");
            }

            // Tạo JSON payload
            JsonObject payload = new JsonObject();
            payload.addProperty("temperature", currentTemp);
            payload.addProperty("time", System.currentTimeMillis());

            MqttMessage message = new MqttMessage(payload.toString().getBytes());
            message.setQos(0); // Gửi nhanh, không cần xác nhận
            mqttClient.publish(topic, message); // không chờ ACK
            System.out.println("📤 Sent: " + payload);

        }
    }
}
