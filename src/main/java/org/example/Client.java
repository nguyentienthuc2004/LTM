package org.example;

import com.google.gson.JsonObject;
import com.google.gson.JsonParser;
import org.eclipse.paho.client.mqttv3.*;
import org.eclipse.paho.client.mqttv3.persist.MemoryPersistence;
import org.java_websocket.client.WebSocketClient;
import org.java_websocket.handshake.ServerHandshake;

import javax.net.ssl.SSLSocketFactory;
import java.net.URI;
import java.nio.charset.StandardCharsets;
import java.util.concurrent.atomic.AtomicInteger;

public class Client {
    public static void main(String[] args) throws Exception {

        // ---------- MQTT config ----------
        String broker = "ssl://8e35c2a9c7114b3f9649e7a3e2e982e3.s1.eu.hivemq.cloud:8883";
        String mqttUser = "thucng04";
        String mqttPass = "Anhthucdz1";
        String dataTopic = "ltm";
        String slowTopic = "ltm/slow";
        String normalTopic = "ltm/normal";

        // ---------- control ----------
        final int NORMAL_DELAY = 0;
        final int SLOW_DELAY = 1000;
        AtomicInteger sendDelay = new AtomicInteger(NORMAL_DELAY);

        // ---------- Binance WebSocket ----------
        String wsUrl = "wss://stream.binance.com:9443/ws/btcusdt@trade";
        String subscribeJson = null; // Binance không cần gói subscribe

        // ---------- MQTT setup ----------
        MqttAsyncClient mqtt = new MqttAsyncClient(broker, MqttClient.generateClientId(), new MemoryPersistence());
        MqttConnectOptions opts = new MqttConnectOptions();
        opts.setUserName(mqttUser);
        opts.setPassword(mqttPass.toCharArray());
        opts.setSocketFactory(SSLSocketFactory.getDefault());
        opts.setAutomaticReconnect(true);
        opts.setCleanSession(true);

        mqtt.setCallback(new MqttCallbackExtended() {
            @Override
            public void connectComplete(boolean reconnect, String serverURI) {
                try {
                    mqtt.subscribe(slowTopic, 1);
                    mqtt.subscribe(normalTopic, 1);
                    System.out.println("✅ MQTT connected & subscribed control topics");
                } catch (MqttException e) {
                    e.printStackTrace();
                }
            }

            @Override
            public void connectionLost(Throwable cause) {
                System.err.println("⚠️ MQTT lost: " + cause);
            }

            @Override
            public void messageArrived(String topic, MqttMessage message) {
                String msg = new String(message.getPayload(), StandardCharsets.UTF_8);
                if (topic.equals(slowTopic)) {
                    sendDelay.set(SLOW_DELAY);
                    System.out.println("🐢 Received SLOW control");
                } else if (topic.equals(normalTopic)) {
                    sendDelay.set(NORMAL_DELAY);
                    System.out.println("🚀 Received NORMAL control");
                }
            }

            @Override
            public void deliveryComplete(IMqttDeliveryToken token) { }
        });

        mqtt.connect(opts).waitForCompletion();
        System.out.println("✅ MQTT connected to broker");

        // ---------- WebSocket client (Binance) ----------
        WebSocketClient ws = new WebSocketClient(new URI(wsUrl)) {
            long lastLog = System.currentTimeMillis();

            @Override
            public void onOpen(ServerHandshake handshakedata) {
                System.out.println("📡 WS connected: " + wsUrl);
                if (subscribeJson != null) send(subscribeJson);
            }

            @Override
            public void onMessage(String message) {
                try {
                    JsonObject root = JsonParser.parseString(message).getAsJsonObject();
                    if (!root.has("p")) return;

                    JsonObject payload = new JsonObject();
                    payload.addProperty("symbol", root.get("s").getAsString());
                    payload.addProperty("price", root.get("p").getAsDouble());
                    payload.addProperty("volume", root.get("q").getAsDouble());
                    payload.addProperty("timestamp", root.get("T").getAsLong());

                    // Publish async (non-blocking)
                    mqtt.publish(dataTopic, payload.toString().getBytes(StandardCharsets.UTF_8), 0, false);

                    // Giảm log để tăng tốc
                    long now = System.currentTimeMillis();
                    if (now - lastLog > 3000) {
                        System.out.println("📤 Example published: " + payload);
                        lastLog = now;
                    }

                    // Nếu chế độ slow được kích hoạt
                    if (sendDelay.get() > 0) Thread.sleep(sendDelay.get());

                } catch (Exception e) {
                    System.err.println("❌ WS->MQTT error: " + e.getMessage());
                }
            }

            @Override
            public void onClose(int code, String reason, boolean remote) {
                System.err.println("⚠️ WS closed: " + reason);
            }

            @Override
            public void onError(Exception ex) {
                System.err.println("❌ WS error: " + ex.getMessage());
            }
        };

        ws.connect();

        // keep main thread alive
        while (true) Thread.sleep(1000);
    }
}
