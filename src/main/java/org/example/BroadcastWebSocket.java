package org.example;

import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import org.java_websocket.WebSocket;
import org.java_websocket.handshake.ClientHandshake;
import org.java_websocket.server.WebSocketServer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.InetSocketAddress;
import java.util.Collection;
import java.util.logging.Level;

public class BroadcastWebSocket extends WebSocketServer {
    private static final Logger log = LoggerFactory.getLogger("BroadcastWebSocket");

    // Cho phép Gson serialize Infinity/-Infinity/NaN
    private static final Gson gson = new GsonBuilder()
            .serializeSpecialFloatingPointValues()
            .create();

    public BroadcastWebSocket(InetSocketAddress addr) {
        super(addr);
    }

    @Override
    public void onOpen(WebSocket conn, ClientHandshake handshake) {
        log.debug("Client connected: " + conn.getRemoteSocketAddress());
    }

    @Override
    public void onClose(WebSocket conn, int code, String reason, boolean remote) {
        log.debug("Client disconnected: " + conn.getRemoteSocketAddress());
    }

    @Override
    public void onMessage(WebSocket conn, String message) { }

    @Override
    public void onError(WebSocket conn, Exception ex) {
        log.debug("WebSocket error", ex);
    }

    @Override
    public void onStart() {
        log.debug("WebSocket server started");
    }

    public void broadcastJson(Object obj) {
        String json = gson.toJson(obj);
        Collection<WebSocket> conns = getConnections(); // WebSocketServer có sẵn
        for (WebSocket c : conns) {
            if (c.isOpen()) c.send(json);
        }
    }
}