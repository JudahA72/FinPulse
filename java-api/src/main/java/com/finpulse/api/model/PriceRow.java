package com.finpulse.api.model;

public class PriceRow {
    public String ticker;
    public String date;
    public Double open;
    public Double high;
    public Double low;
    public Double close;
    public Double adjClose;
    public Long volume;

    public PriceRow(
            String ticker,
            String date,
            Double open,
            Double high,
            Double low,
            Double close,
            Double adjClose,
            Long volume
    ) {
        this.ticker = ticker;
        this.date = date;
        this.open = open;
        this.high = high;
        this.low = low;
        this.close = close;
        this.adjClose = adjClose;
        this.volume = volume;
    }
}
