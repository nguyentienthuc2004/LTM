package org.example;

public class TradeData {
    private final double price;
    private final double quantity;
    private final long time;

    public TradeData(double price, double quantity, long time) {
        this.price = price;
        this.quantity = quantity;
        this.time = time;
    }

    public double getPrice() {
        return price;
    }

    public double getQuantity() {
        return quantity;
    }

    public long getTime() {
        return time;
    }

    public boolean isValid() { return price > 0 && quantity > 0; }
}
