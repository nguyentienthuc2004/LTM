package org.example;

import com.google.gson.JsonObject;
import org.eclipse.paho.client.mqttv3.*;
import org.eclipse.paho.client.mqttv3.persist.MemoryPersistence;

import javax.net.ssl.SSLSocketFactory;
import java.util.concurrent.ThreadLocalRandom;

public class Client {
    public static void main(String[] args) throws Exception {
        // MQTT config HiveMQ Cloud
        String broker = "ssl://0ac901938c97430b9bde1e30ea590141.s1.eu.hivemq.cloud:8883";
        String username = "thucng04";
        String password = "Anhthucdz1";
        String topic = "ltm";

        MqttClient mqttClient = new MqttClient(broker, MqttClient.generateClientId(), new MemoryPersistence());
        MqttConnectOptions options = new MqttConnectOptions();
        options.setUserName(username);
        options.setPassword(password.toCharArray());
        options.setSocketFactory(SSLSocketFactory.getDefault());
        options.setAutomaticReconnect(true);
        mqttClient.connect(options);
        System.out.println("Connected to MQTT HiveMQ Cloud");

        double basePrice = 115000;  // base price để demo
        double baseQty = 0.01;

        while (true) {
            ThreadLocalRandom rnd = ThreadLocalRandom.current();

            // tạo biến động ±5%
            double price = basePrice * (0.95 + rnd.nextDouble() * 0.10);
            double quantity = baseQty * (0.5 + rnd.nextDouble());

            // Thêm “spike” ngẫu nhiên
            if (rnd.nextInt(0, 20) == 0) {  // 5% chance
                price *= 1 + rnd.nextDouble() * 0.2;  // +0~20% spike
                quantity *= 1 + rnd.nextDouble() * 2; // +0~200% spike
            }

            long timestamp = System.currentTimeMillis();

            // build payload JSON
            JsonObject payload = new JsonObject();
            payload.addProperty("price", price);
            payload.addProperty("quantity", quantity);
            payload.addProperty("time", timestamp);

            // publish lên MQTT
            if (mqttClient.isConnected()) {
                mqttClient.publish(topic, new MqttMessage(payload.toString().getBytes()));
            }

            System.out.printf("Published: price=%.2f, qty=%.5f%n", price, quantity);

            Thread.sleep(200); // ~5 msg/s
        }
    }
}
