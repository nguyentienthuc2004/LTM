package org.example;

public class StatsCollector {
    private int count = 0;
    private double sumPrice = 0;
    private double sumQty = 0;
    private double maxPrice = Double.NEGATIVE_INFINITY;
    private double minPrice = Double.POSITIVE_INFINITY;

    void collect(TradeData d) {
        count++;
        sumPrice += d.getPrice();
        sumQty += d.getQuantity();
        maxPrice = Math.max(maxPrice, d.getPrice());
        minPrice = Math.min(minPrice, d.getPrice());
    }

    public double avgPrice() { return count == 0 ? 0 : sumPrice / count; }
    public double avgQuantity() { return count == 0 ? 0 : sumQty / count; }
    public int getCount() { return count; }
    public double getMaxPrice() { return maxPrice; }
    public double getMinPrice() { return minPrice; }
}
